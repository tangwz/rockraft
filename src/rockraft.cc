#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <gflags/gflags.h>               // DEFINE_*
#include <brpc/controller.h>             // brpc::Controller
#include <brpc/server.h>                 // brpc::Server
#include <butil/files/file_enumerator.h>
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include "rocksdb/db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rockraft.pb.h"                 // RockRaftService

DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms, 5000, "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(port, 8000, "Listen port of this peer");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DEFINE_string(db_path, "./data/rocksdb_data", "Path of rocksdb");
DEFINE_string(group, "RockRaft", "Id of the replication group");

namespace rockraft {
    class RockRaft;

// Implements Closure which encloses RPC stuff
    class RockRaftClosure : public braft::Closure {
    public:
        RockRaftClosure(RockRaft* rockraft, RockRaftResponse* response,
                        google::protobuf::Closure* done)
                : _rockraft(rockraft)
                , _response(response)
                , _done(done) {}
        ~RockRaftClosure() {}

        RockRaftResponse* response() const { return _response; }
        void Run();

    private:
        RockRaft* _rockraft;
        RockRaftResponse* _response;
        google::protobuf::Closure* _done;
    };

    bool copy_snapshot(const std::string& from_path, const std::string& to_path) {
        struct stat from_stat;
        if (stat(from_path.c_str(), &from_stat) < 0 || !S_ISDIR(from_stat.st_mode)) {
            LOG(WARNING) << "stat " << from_path << " failed";
            return false;
        }

        if (!butil::CreateDirectory(butil::FilePath(to_path))) {
            LOG(WARNING) << "CreateDirectory " << to_path << " failed";
            return false;
        }

        butil::FileEnumerator dir_enum(butil::FilePath(from_path),
                                       false, butil::FileEnumerator::FILES);
        for (butil::FilePath name = dir_enum.Next(); !name.empty(); name = dir_enum.Next()) {
            std::string src_file(from_path);
            std::string dst_file(to_path);
            butil::string_appendf(&src_file, "/%s", name.BaseName().value().c_str());
            butil::string_appendf(&dst_file, "/%s", name.BaseName().value().c_str());

            if (0 != link(src_file.c_str(), dst_file.c_str())) {
                if (!butil::CopyFile(butil::FilePath(src_file), butil::FilePath(dst_file))) {
                    LOG(WARNING) << "copy " << src_file << " to " << dst_file << " failed";
                    return false;
                }
            }
        }

        return true;
    }

// Implementation of RockRaft as a braft::StateMachine
    class RockRaft : public braft::StateMachine {
    public:
        RockRaft()
                : _node(NULL)
                , _leader_term(-1)
                , _db(NULL)
        {

        }

        ~RockRaft() {
            delete _node;
            delete _db;
        }

        // Start this node
        int start() {
            std::string db_path = FLAGS_db_path;
            if (!butil::DeleteFile(butil::FilePath(db_path), true)) {
                LOG(ERROR) << "Delete file " << db_path << " failed";
                return -1;
            }

            // init_rocksdb() MUST before Node::init, maybe single node become
            // leader and restore from raft's snapshot, the restore log.
            if (init_rocksdb() != 0) {
                return -1;
            }

            butil::EndPoint addr(butil::my_ip(), FLAGS_port);
            braft::NodeOptions node_options;
            if (node_options.initial_conf.parse_from(FLAGS_conf) != 0) {
                LOG(ERROR) << "Fail to parse configuration `" << FLAGS_conf << '\'';
                return -1;
            }
            node_options.election_timeout_ms = FLAGS_election_timeout_ms;
            node_options.fsm = this;
            node_options.node_owns_fsm = false;
            node_options.snapshot_interval_s = FLAGS_snapshot_interval;
            std::string prefix = "local://" + FLAGS_data_path;
            node_options.log_uri = prefix + "/log";
            node_options.raft_meta_uri = prefix + "/raft_meta";
            node_options.snapshot_uri = prefix + "/snapshot";
            node_options.disable_cli = FLAGS_disable_cli;
            braft::Node* node = new braft::Node(FLAGS_group, braft::PeerId(addr));
            if (node->init(node_options) != 0) {
                LOG(ERROR) << "Fail to init raft node";
                delete node;
                return -1;
            }
            _node = node;

            return 0;
        }

        // Implements Service methods
        void put(const PutRequest* request,
                 RockRaftResponse* response,
                 google::protobuf::Closure* done) {
            brpc::ClosureGuard done_guard(done);
            // Serialize request to the replicated write-ahead-log so that all the
            // peers in the group receive this request as well.
            // Notice that _value can't be modified in this routine otherwise it
            // will be inconsistent with others in this group.

            // Serialize request to IOBuf
            const int64_t term = _leader_term.load(butil::memory_order_relaxed);
            if (term < 0) {
                return redirect(response);
            }

            butil::IOBuf log;
            butil::IOBufAsZeroCopyOutputStream wrapper(&log);
            if (!request->SerializeToZeroCopyStream(&wrapper)) {
                LOG(ERROR) << "Fail to serialize request";
                response->set_success(false);
                return;
            }

            // Apply this log as a braft::Task
            braft::Task task;
            task.data = &log;
            // This callback would be iovoked when the task actually excuted or
            // fail
            task.done = new RockRaftClosure(this, response, done_guard.release());
            if (FLAGS_check_term) {
                // ABA problem can be avoid if expected_term is set
                task.expected_term = term;
            }
            // Now the task is applied to the group, waiting for the result.
            return _node->apply(task);
        }

        void get(const std::string& key, RockRaftResponse* response) {
            // In consideration of consistency, GetRequest to follower should be
            // rejected.
            // Reference: https://github.com/baidu/braft/blob/master/example/counter/server.cpp
            if (!is_leader()) {
                return redirect(response);
            }

            CHECK(_db != NULL);

            std::string value;
            rocksdb::Status status = _db->Get(rocksdb::ReadOptions(), key, &value);
            if (status.ok()) {
                LOG(NOTICE) << "this is leader, get request, key: " << key << " value: " << value;
                response->set_success(true);
                response->set_value(value);
            } else {
                LOG(WARNING) << "get failed, key: " << key;
                response->set_success(false);
            }

            return;
        }

        bool is_leader() const {
            return _leader_term.load(butil::memory_order_acquire) > 0;
        }

        // Shut this node down.
        void shutdown() {
            if (_node) {
                _node->shutdown(NULL);
            }
        }

        // Blocking this thread until the node is eventually down.
        void join() {
            if (_node) {
                _node->join();
            }
        }

    private:
        friend class RockRaftClosure;

        void redirect(RockRaftResponse* response) {
            response->set_success(false);
            if (_node) {
                braft::PeerId leader = _node->leader_id();
                if (!leader.is_empty()) {
                    response->set_redirect(leader.to_string());
                }
            }
        }

        // @braft::StateMachine
        void on_apply(braft::Iterator& iter) {
            // A batch of tasks are committed, which must be processed through
            // |iter|
            for (; iter.valid(); iter.next()) {
                // This guard helps invoke iter.done()->Run() asynchronously to
                // avoid that callback blocks the StateMachine.
                braft::AsyncClosureGuard closure_guard(iter.done());
                const butil::IOBuf& data = iter.data();

                RockRaftResponse* response = NULL;
                if (iter.done()) {
                    // This task is applied by this node, get value from this
                    // closure to avoid additional parsing.
                    RockRaftClosure* c = dynamic_cast<RockRaftClosure*>(iter.done());
                    response = c->response();
                    // braft::run_closure_in_bthread_nosig(closure_guard.release());
                }

                fsm_put(dynamic_cast<RockRaftClosure*>(iter.done()), data);
                if (response) {
                    response->set_success(true);
                }

                // The purpose of following logs is to help you understand the way
                // this StateMachine works.
                // Remove these logs in performance-sensitive servers.
                LOG_IF(INFO, FLAGS_log_applied_task)
                        << " at log_index=" << iter.index();
            }
            // bthread_flush();
        }

        void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
            CHECK(_db != NULL);
            // Serialize StateMachine to the snapshot
            brpc::ClosureGuard done_guard(done);
            std::string snapshot_path = writer->get_path() + "/rocksdb_snapshot";

            LOG(INFO) << "Saving snapshot to " << snapshot_path;

            // Use rocksdb checkpoint implement snapshot.
            rocksdb::Checkpoint* checkpoint = NULL;
            rocksdb::Status status = rocksdb::Checkpoint::Create(_db, &checkpoint);
            if (!status.ok()) {
                LOG(WARNING) << "Checkpoint Create failed, msg: " << status.ToString();
                return;
            }

            std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);
            status = checkpoint->CreateCheckpoint(snapshot_path);
            if (!status.ok()) {
                LOG(WARNING) << "Checkpoint CreateCheckpoint failed, msg: " << status.ToString();
                return;
            }

            butil::FileEnumerator dir_enum(butil::FilePath(snapshot_path),
                                           false, butil::FileEnumerator::FILES);
            for (butil::FilePath name = dir_enum.Next(); !name.empty(); name = dir_enum.Next()) {
                std::string file_name = "rocksdb_snapshot/" + name.BaseName().value();
                if (writer->add_file(file_name) != 0) {
                    done->status().set_error(EIO, "Fail to add file to writer");
                    return;
                }
            }
        }

        int on_snapshot_load(braft::SnapshotReader* reader) {
            if (_db != NULL) {
                delete _db;
                _db = NULL;
            }

            std::string snapshot_path = reader->get_path();
            snapshot_path.append("/rocksdb_snapshot");

            // we delete the old rocksdb data and use snapshot and wal
            // rebuild a new db.
            std::string db_path = FLAGS_db_path;
            if(!butil::DeleteFile(butil::FilePath(db_path), true)) {
                return -1;
            }

            if (!copy_snapshot(snapshot_path, db_path)) {
                return -1;
            }

            return init_rocksdb();
        }

        void on_leader_start(int64_t term) {
            _leader_term.store(term, butil::memory_order_release);
            LOG(INFO) << "Node becomes leader";
        }

        void on_leader_stop(const butil::Status& status) {
            _leader_term.store(-1, butil::memory_order_release);
            LOG(INFO) << "Node stepped down : " << status;
        }

        void on_shutdown() {
            LOG(INFO) << "This node is down";
        }

        void on_error(const ::braft::Error& e) {
            LOG(ERROR) << "Met raft error " << e;
        }
        void on_configuration_committed(const ::braft::Configuration& conf) {
            LOG(INFO) << "Configuration of this group is " << conf;
        }
        void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
            LOG(INFO) << "Node stops following " << ctx;
        }
        void on_start_following(const ::braft::LeaderChangeContext& ctx) {
            LOG(INFO) << "Node start following " << ctx;
        }
        // end of @braft::StateMachine

        int init_rocksdb() {
            if (_db != NULL) {
                LOG(INFO) << "rocksdb already opened";
                return 0;
            }

            std::string db_path = FLAGS_db_path;
            if (!butil::CreateDirectory(butil::FilePath(db_path))) {
                return -1;
            }

            rocksdb::Options options;
            options.create_if_missing = true;
            rocksdb::Status status = rocksdb::DB::Open(options, db_path, &_db);
            if (!status.ok()) {
                LOG(ERROR) << "open rocksdb " << db_path << " failed, msg: " << status.ToString();
                return -1;
            }

            LOG(INFO) << "rocksdb open success!";
            return 0;
        }

        void fsm_put(RockRaftClosure* done, const butil::IOBuf& data) {
            butil::IOBufAsZeroCopyInputStream wrapper(data);

            PutRequest req;
            CHECK(req.ParseFromZeroCopyStream(&wrapper));

            // WriteOptions not need WAL, use raft's WAL.
            rocksdb::WriteOptions options;
            options.disableWAL = true;
            LOG(INFO) << "rocksdb PUT key: " << req.key() << " value: " << req.value();
            rocksdb::Status status = _db->Put(options, req.key(), req.value());
            if (status.ok()) {
                LOG(INFO) << "Set success, key: " << req.key() << " value: " << req.value();
            } else {
                if (done) {
                    done->status().set_error(brpc::EREQUEST, status.ToString());
                }
                LOG(ERROR) << "Set failed, key: " << req.key() << " value: " << req.value();
            }
        }

    private:
        braft::Node* volatile _node;
        butil::atomic<int64_t> _leader_term;
        rocksdb::DB* _db;
    };

    void RockRaftClosure::Run() {
        // Auto delete this after Run()
        std::unique_ptr<RockRaftClosure> self_guard(this);
        // Repsond this RPC.
        brpc::ClosureGuard done_guard(_done);
        if (status().ok()) {
            return;
        }
        // Try redirect if this request failed.
        _rockraft->redirect(_response);
    }

// Implements RockRaftService
    class RockRaftServiceImpl : public RockRaftService {
    public:
        explicit RockRaftServiceImpl(RockRaft* rockraft) : _rockraft(rockraft) {}

        void put(::google::protobuf::RpcController* controller,
                 const ::rockraft::PutRequest* request,
                 ::rockraft::RockRaftResponse* response,
                 ::google::protobuf::Closure* done) {
            LOG(INFO) << "PutRequest key: " << request->key() << " value: " << request->value();
            return _rockraft->put(request, response, done);
        }

        void get(::google::protobuf::RpcController* controller,
                 const ::rockraft::GetRequest* request,
                 ::rockraft::RockRaftResponse* response,
                 ::google::protobuf::Closure* done) {
            LOG(INFO) << "GetRequest key: " << request->key();
            brpc::ClosureGuard done_guard(done);
            return _rockraft->get(request->key(), response);
        }

    private:
        RockRaft* _rockraft;
    };

} // namespace rockraft


int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Generally you only need one Server.
    brpc::Server server;
    rockraft::RockRaft rockraft;
    rockraft::RockRaftServiceImpl service(&rockraft);

    // Add your service into RPC server
    if (server.AddService(&service,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    // raft can share the same RPC server. Notice the second parameter, because
    // adding services into a running server is not allowed and the listen
    // address of this server is impossible to get before the server starts. You
    // have to specify the address of the server.
    if (braft::add_service(&server, FLAGS_port) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    // It's recommended to start the server before RockRaft is started to avoid
    // the case that it becomes the leader while the service is unreacheable by
    // clients.
    // Notice the default options of server is used here. Check out details from
    // the doc of brpc if you would like change some options;
    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }

    // It's ok to start RockRaft;
    if (rockraft.start() != 0) {
        LOG(ERROR) << "Fail to start RockRaft";
        return -1;
    }

    LOG(INFO) << "RockRaft service is running on " << server.listen_address();
    // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }

    LOG(INFO) << "RockRaft service is going to quit";

    // Stop rockraft before server
    rockraft.shutdown();
    server.Stop(0);

    // Wait until all the processing tasks are over.
    rockraft.join();
    server.Join();
    return 0;
}
