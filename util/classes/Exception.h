#ifndef LEVELDB_UTIL_EXCEPTION_H
#define LEVELDB_UTIL_EXCEPTION_H

/**
 * @file Exception.h
 */

// System includes

#include <iostream>
#include <sstream>
#include <string>

#ifdef ThrowError
#undef ThrowError
#endif

#define ThrowError(text) \
{\
  std::ostringstream _macroOs; \
  _macroOs << text;\
  gcp::util::ErrHandler::throwError(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, false, false, false); \
}

#define ThrowErrorHelp(text) \
{\
  std::ostringstream _macroOs; \
  _macroOs << text;\
  gcp::util::ErrHandler::throwError(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, false, false, true); \
}

#ifdef ThrowSimpleError
#undef ThrowSimpleError
#endif

#define ThrowSimpleError(text) \
{\
  std::ostringstream _macroOs; \
  _macroOs << text;\
  gcp::util::ErrHandler::throwError(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, true, false, false); \
}

#define ThrowSimpleErrorHelp(text) \
{\
  std::ostringstream _macroOs; \
  _macroOs << text;\
  gcp::util::ErrHandler::throwError(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, true, false, true); \
}

#define ThrowColorError(text, color)                    \
  {\
    gcp::util::XtermManip _macroXtm;		\
    std::ostringstream _macroOs; \
    _macroOs << _macroXtm.bg("black") << _macroXtm.fg(color) << _macroXtm.textMode("bold");\
    _macroOs << text;\
    _macroOs << _macroXtm.bg("default") << _macroXtm.fg("default") << _macroXtm.textMode("normal");\
    gcp::util::ErrHandler::throwError(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, false, false, false); \
  }

#define ThrowColorErrorHelp(text, color)                    \
  {\
    gcp::util::XtermManip _macroXtm;		\
    std::ostringstream _macroOs; \
    _macroOs << _macroXtm.bg("black") << _macroXtm.fg(color) << _macroXtm.textMode("bold");\
    _macroOs << text;\
    _macroOs << _macroXtm.bg("default") << _macroXtm.fg("default") << _macroXtm.textMode("normal");\
    gcp::util::ErrHandler::throwError(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, false, false, true); \
  }

#define ThrowSimpleColorError(text, color)                    \
  {\
    gcp::util::XtermManip _macroXtm;		\
    std::ostringstream _macroOs; \
    _macroOs << _macroXtm.bg("black") << _macroXtm.fg(color) << _macroXtm.textMode("bold");\
    _macroOs << text;\
    _macroOs << _macroXtm.bg("default") << _macroXtm.fg("default") << _macroXtm.textMode("normal");\
    gcp::util::ErrHandler::throwError(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, true, false, false); \
  }

#define ThrowSimpleColorErrorHelp(text, color)                    \
  {\
    gcp::util::XtermManip _macroXtm;		\
    std::ostringstream _macroOs; \
    _macroOs << _macroXtm.bg("black") << _macroXtm.fg(color) << _macroXtm.textMode("bold");\
    _macroOs << text;\
    _macroOs << _macroXtm.bg("default") << _macroXtm.fg("default") << _macroXtm.textMode("normal");\
    gcp::util::ErrHandler::throwError(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, true, false, true); \
  }


#ifdef ThrowSysError
#undef ThrowSysError
#endif

#define ThrowSysError(text) \
{\
  std::ostringstream _macroOs; \
  _macroOs << text;\
  gcp::util::ErrHandler::throwError(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, false, true, false); \
}

#define ThrowSysErrorHelp(text) \
{\
  std::ostringstream _macroOs; \
  _macroOs << text;\
  gcp::util::ErrHandler::throwError(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, false, true, true); \
}

#ifdef ReportError
#undef ReportError
#endif

#define ReportError(text) \
{\
  std::ostringstream _macroOs; \
  _macroOs << text;\
  gcp::util::ErrHandler::report(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, false, false, false); \
}

#ifdef ReportSimpleError
#undef ReportSimpleError
#endif

#define ReportSimpleError(text) \
{\
  std::ostringstream _macroOs; \
  _macroOs << text;\
  gcp::util::ErrHandler::report(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, true, false, false);	\
}

#ifdef ReportSimpleColorError
#undef ReportSimpleColorError
#endif

#define ReportSimpleColorError(text, color)		\
{\
  gcp::util::XtermManip _macroXtm;		\
  std::ostringstream _macroOs; \
  _macroOs << _macroXtm.bg("black") << _macroXtm.fg(color) << _macroXtm.textMode("bold");\
  _macroOs << text;\
  _macroOs << _macroXtm.bg("default") << _macroXtm.fg("default") << _macroXtm.textMode("normal");\
  gcp::util::ErrHandler::report(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, true, false, false);	\
}

#ifdef ReportSysError
#undef ReportSysError
#endif

#define ReportSysError(text) \
{\
  std::ostringstream _macroOs; \
  _macroOs << text;\
  gcp::util::ErrHandler::report(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, true, false, true, false);	\
}

#ifdef ReportMessage
#undef ReportMessage
#endif

#define ReportMessage(text) \
{\
  std::ostringstream _macroOs; \
  _macroOs << text;\
  gcp::util::ErrHandler::report(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, false, true, false, false); \
}

#ifdef LogMessage
#undef LogMessage
#endif

#define LogMessage(error, text) \
{\
  std::ostringstream _macroOs; \
  _macroOs << text;\
  gcp::util::ErrHandler::log(_macroOs, __FILE__, __LINE__, __PRETTY_FUNCTION__, false, true, false, false); \
}

#define COUT(statement) \
{\
    std::ostringstream _macroOs; \
    _macroOs << statement << std::endl; \
    gcp::util::Logger::printToStdout(_macroOs.str()); \
}

#define FORMATCOLOR(_inputOs, statement, color)	\
{\
    gcp::util::XtermManip _macroXtm; \
    _inputOs << _macroXtm.bg("black") << _macroXtm.fg(color) << _macroXtm.textMode("bold");\
    _inputOs << statement << std::endl;					\
    _inputOs << _macroXtm.bg("default") << _macroXtm.fg("default") << _macroXtm.textMode("normal");\
}

#define COLORIZE(xtm, color, statement)				\
  xtm.bg("black") << xtm.fg(color) << xtm.textMode("bold") << statement << xtm.bg("default") << xtm.fg("default") << xtm.textMode("normal")

#define COUTCOLOR(statement, color) \
{\
    gcp::util::XtermManip _macroXtm; \
    std::ostringstream _macroOs; \
    _macroOs << _macroXtm.bg("black") << _macroXtm.fg(color) << _macroXtm.textMode("bold");\
    _macroOs << statement << std::endl;	\
    _macroOs << _macroXtm.bg("default") << _macroXtm.fg("default") << _macroXtm.textMode("normal"); \
    gcp::util::Logger::printToStdout(_macroOs.str()); \
}

#define COUTCOLORNNL(statement, color) \
{\
    gcp::util::XtermManip _macroXtm; \
    std::ostringstream _macroOs; \
    _macroOs << _macroXtm.bg("black") << _macroXtm.fg(color) << _macroXtm.textMode("bold");\
    _macroOs << statement;	\
    _macroOs << _macroXtm.bg("default") << _macroXtm.fg("default") << _macroXtm.textMode("normal");\
    gcp::util::Logger::printToStdout(_macroOs.str()); \
}

#define CTOUT(statement) \
{\
    gcp::util::TimeVal _macroTimeVal;\
    _macroTimeVal.setToCurrentTime();\
    std::ostringstream _macroOs; \
    _macroOs << _macroTimeVal << ": " << statement << std::endl; \
    gcp::util::Logger::printToStdout(_macroOs.str()); \
}

#define CERR(statement) \
{\
    std::ostringstream _macroOs; \
    _macroOs << statement << std::endl; \
    gcp::util::Logger::printToStderr(_macroOs.str()); \
}

#define CTERR(statement) \
{\
    gcp::util::TimeVal _macroTimeVal;\
    _macroTimeVal.setToCurrentTime();\
    std::ostringstream _macroOs; \
    _macroOs << _macroTimeVal << ": " << statement << std::endl; \
    gcp::util::Logger::printToStderr(_macroOs.str()); \
}

#endif
