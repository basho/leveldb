class AtExit
{
public:
    typedef std::function<void()> Function;

    explicit AtExit(Function &&f): f_(std::move(f)) {}
    ~AtExit() { f_(); }

private:
    Function f_;
};
