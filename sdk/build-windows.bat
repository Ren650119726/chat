@echo off

:: Set the necessary environment variables
set GOARCH=amd64
set GOOS=windows
set CGO_ENABLED=1

go build -x -v -buildmode=c-shared -o im.dll im.go callback.go

echo Compilation finished.
pause
