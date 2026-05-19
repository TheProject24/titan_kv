use chrono::Local;
use colored::*;

pub enum LogLevel {
    INFO,
    WARN,
    ERROR,
    DEBUG,
    SUCCESS,
}

pub fn log(level: LogLevel, context: &str, message: &str) {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    
    let level_str = match level {
        LogLevel::INFO => "[INFO]".cyan().bold(),
        LogLevel::WARN => "[WARN]".yellow().bold(),
        LogLevel::ERROR => "[ERR!]".red().bold().blink(),
        LogLevel::DEBUG => "[DBUG]".magenta().bold(),
        LogLevel::SUCCESS=> "[ OK ]".green().bold(),
    };

    let ctx_str = format!("[{}]", context).truecolor(150, 150, 150);

    match level {
        LogLevel::ERROR => {
            eprintln!("{} {} {} {}", timestamp.dimmed(), level_str, ctx_str, message.red());
        }
        LogLevel::WARN => {
            println!("{} {} {} {}", timestamp.dimmed(), level_str, ctx_str, message.yellow());
        }
        LogLevel::SUCCESS => {
            println!("{} {} {} {}", timestamp.dimmed(), level_str, ctx_str, message.green());
        }
        _ => {
            println!("{} {} {} {}", timestamp.dimmed(), level_str, ctx_str, message.white());
        }
    }
}

#[macro_export]
macro_rules! log_info {
    ($ctx:expr, $($arg:tt)*) => {
        $crate::logger::log($crate::logger::LogLevel::INFO, $ctx, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_warn {
    ($ctx:expr, $($arg:tt)*) => {
        $crate::logger::log($crate::logger::LogLevel::WARN, $ctx, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_error {
    ($ctx:expr, $($arg:tt)*) => {
        $crate::logger::log($crate::logger::LogLevel::ERROR, $ctx, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_debug {
    ($ctx:expr, $($arg:tt)*) => {
        $crate::logger::log($crate::logger::LogLevel::DEBUG, $ctx, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_success {
    ($ctx:expr, $($arg:tt)*) => {
        $crate::logger::log($crate::logger::LogLevel::SUCCESS, $ctx, &format!($($arg)*))
    };
}
