#include <unistd.h>
#include <assert.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "log_manager.h"

LogManager::LogManager(DiskManager* disk_manager, LogReplay* log_replay)
        :disk_manager_(disk_manager), log_replay_(log_replay) {
    // if(!disk_manager_->is_file(LOG_FILE_NAME)) {
    //     disk_manager_->create_file(LOG_FILE_NAME);
    // }
    log_file_fd_ = disk_manager_->open_file(LOG_FILE_NAME);
    // batch_id_t init_batch_id = INVALID_BATCH_ID;
    // size_t init_persist_off = sizeof(batch_id_t) + sizeof(size_t);
    // write(log_file_fd_, &init_batch_id, sizeof(batch_id_t));
    // write(log_file_fd_, &init_persist_off, sizeof(size_t));
}

void LogManager::write_batch_log_to_disk(std::string batch_log) {
    if (log_file_fd_ == -1) {
        log_file_fd_ = disk_manager_->open_file(LOG_FILE_NAME);
    }

    lseek(log_file_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_file_fd_, batch_log.c_str(), batch_log.length() * sizeof(char));
    assert(bytes_write == batch_log.length() * sizeof(char));

    // RDMA_LOG(INFO) << "Write batch log's size is " << bytes_write;

    log_replay_->add_max_replay_off_(bytes_write);
}

void LogManager::write_batch_log_to_disk(char* batch_log, size_t size) {
    if (log_file_fd_ == -1) {
        log_file_fd_ = disk_manager_->open_file(LOG_FILE_NAME);
    }

    lseek(log_file_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_file_fd_, batch_log, size);
    assert(bytes_write == size);

    // RDMA_LOG(INFO) << "Write batch log's size is " << bytes_write;

    log_replay_->add_max_replay_off_(bytes_write);
}