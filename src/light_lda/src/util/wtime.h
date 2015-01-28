#pragma once
#include <ctime>
#include <pthread.h>
#include <WinSock2.h>

namespace petuum
{
int nanosleep(const struct timespec *request, struct timespec *remain);
//int clock_gettime(int X, struct timeval *tv);
int clock_gettime(int X, struct timespec *tv);

}