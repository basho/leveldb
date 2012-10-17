namespace leveldb {

// background routines to close and/or unmap files
void BGFileCloser(void* file_info);
void BGFileCloser2(void* file_info);
void BGFileUnmapper(void* file_info);
void BGFileUnmapper2(void* file_info);

// data needed by background routines for close/unmap
struct BGCloseInfo
{
    int fd_;
    void * base_;
    size_t offset_;
    size_t length_;
    size_t unused_;

    BGCloseInfo(int fd, void * base, size_t offset, size_t length, size_t unused)
        : fd_(fd), base_(base), offset_(offset), length_(length), unused_(unused) {};
};

};  // namespace leveldb

