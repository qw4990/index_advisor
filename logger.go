package main

import (
	"fmt"
	"strings"
)

var (
	// 0 for debug, 1 for info, 2 for warning, 3 for error
	logLevel = 0
)

func updateLogLevel(level string) {
	if level == "" {
		return // use default
	}
	level = strings.TrimSpace(strings.ToLower(level))
	switch level {
	case "debug":
		logLevel = 0
	case "info":
		logLevel = 1
	case "warning":
		logLevel = 2
	case "error":
		logLevel = 3
	default:
		panic("invalid log level: " + level)
	}
}

func Debugf(format string, args ...interface{}) {
	if logLevel <= 0 {
		log("DEBUG", format, args...)
	}
}

func Infof(format string, args ...interface{}) {
	if logLevel <= 1 {
		log("INFO", format, args...)
	}
}

func Warningf(format string, args ...interface{}) {
	if logLevel <= 2 {
		log("WARNING", format, args...)
	}
}

func Errorf(format string, args ...interface{}) {
	if logLevel <= 3 {
		log("ERROR", format, args...)
	}
}

func log(level, format string, args ...interface{}) {
	fmt.Printf("[%v] %v\n", level, fmt.Sprintf(format, args...))
}
