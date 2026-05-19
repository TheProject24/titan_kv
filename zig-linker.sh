#!/bin/sh
# Filter out -nolibc and --unwindlib=none so zig can provide its bundled Windows C runtime
args=""
for arg in "$@"; do
    case "$arg" in
        -nolibc|--unwindlib=*) ;;
        *) args="$args $arg" ;;
    esac
done
exec zig cc -target x86_64-windows-gnu $args
