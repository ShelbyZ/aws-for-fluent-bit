#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>    
#include <unistd.h>
#include <string.h>
#include <math.h>

// Large text around 1Kb
#define ONE_KB_TEXT "RUDQEWDDKBVMHPYVOAHGADVQGRHGCNRDCTLUWQCBFBKFGZHTGEUKFXWNCKXPRWBSVJGHEARMDQGVVRFPVCIBYEORHYPUTQJKUMNZJXIYLDCJUHABJIXFPUNJQDORGPKWFLQZXIGVGCWTZCVWGBFSGVXGEITYKNTWCYZDOAZFOTXDOFRPECXBSCSORSUUNUJZEJZPTODHBXVMOETBRFGNWNZHGINVNYZPKKSFLZHLSSDHFGLTHZEKICPGNYSCTAIHARDDYIJHKLMAOIDLEKRXMFNVJOJVDFYKNVIQKCIGTRFWKJRHQSFDWWKTJNMNKFBOMBMZMRCOHPUFZEPTQTZBLBDBZPJJXRYDFSOWKDVZLZYWSJYFTCKQJFPQOMCWQHKLNHUGWWVBGTRLLVUHTPHTKNBSRUNNOIFGIJPBHPCKYXNGDCQYJEWFFKRRTHJDUBEZPJIXMAOLZQDZQAYEUZFRLTLTXNGAVAGZZDUERZWTJVDTXPKOIRTCKTFOFJAXVFLNKPBYOIYVPHUYBRZZORCEMMAUTZIAUSXVDTKHSUIRTSYWQMYZBMUGSATXPNESEVQMUKHYZFWSLHJDNYUQWOKDUTUKPRXBLIYGSCFGBGXATINMMCWNWBGJTLZTPKGBTPWTHQPUHDJITWPCJLGZFNZTCIEWWVTREFCTPVOUADQCRQCBRHNHDKGQIXHIWGGDGAAFYZRODKFTKQATAUDOMZTSQUYZHGNJOBSUJDHESPBOIJCGXPEZMMQJNFTYBJEYXPZAZICZJKEZKCZEUMZTTSQEHADOVMCDMDEBUJAPKIAEYQEWIYZSAYAWAGFSTBJYCUFZHMJMLCTVTZWGCPDAURQYSXVICLVWKPAOMVTQTESYFPTMNMSNZPUXMDJRDKHDRAIRYELEXRJUAMOLZVWNHGNVFETVUDZEIDJRPSHMXAZDZXDCXMUJTPDTDUHBAZGPIQOUNUHMVLCZCSUUHGTEIQOUNUHMVLCZCSUUHGTEIQOU"

int getMessagesPerCycle(int throughput, float size, float burst)
{
    return ceil(throughput / size) * (60000 / (60000 - burst));
}

int getTimeBetweenCycle(int throughput, float size, float burst)
{
    return floor((size / throughput) * ((60000 - burst) / 60000)) * 1000;
}

long long timeInMilliseconds(void) {
    struct timeval tv;

    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000)+(tv.tv_usec/1000);
}

/* msleep(): Sleep for the requested number of milliseconds. */
void msleep(long msec)
{
    struct timespec ts;
    int res;

    if (msec < 0)
    {
        errno = EINVAL;
    }

    ts.tv_sec = msec / 1000;
    ts.tv_nsec = (msec % 1000) * 1000000;

    do {
        res = nanosleep(&ts, &ts);
    } while (res && errno == EINTR);
}

int main()
{
    int sizeInKb = atoi(getenv("SIZE_IN_KB"));
    int totalSizeInKb = atoi(getenv("TOTAL_SIZE_IN_MB")) * 1024;
    int throughputInKb = atoi(getenv("THROUGHPUT_IN_KB"));
    int burstDelayInMs = atoi(getenv("BURST_DELAY_IN_SECONDS")) * 1000;

    int idCounter = 10000000;
    char* data = malloc(strlen(ONE_KB_TEXT) * sizeInKb + 1);

    int totalMessages = totalSizeInKb / sizeInKb;
    int messagesSent = 0;
    int timeBetweenCycleInMs = getTimeBetweenCycle(throughputInKb, sizeInKb, burstDelayInMs);
    int messagesPerSecond = getMessagesPerCycle(throughputInKb, sizeInKb, burstDelayInMs);
    int burstDelayPerCycleInMs = burstDelayInMs / totalMessages;

    // build log message size
    for (int i = 0; i < sizeInKb; i++)
    {
        snprintf(&data[strlen(data)], strlen(ONE_KB_TEXT) + 1, "%s", ONE_KB_TEXT);
    }

    printf("sizeInKb: %d\n", sizeInKb);
    printf("totalSizeInKb: %d\n", totalSizeInKb);
    printf("throughputInKb: %d\n", throughputInKb);
    printf("burstDelayInMs: %d\n", burstDelayInMs);
    printf("timeBetweenCycleInMs: %d\n", timeBetweenCycleInMs);
    printf("messagesPerSecond: %d\n", messagesPerSecond);
    printf("totalMessages: %d\n", totalMessages);
    printf("burstDelayPerCycleInMs: %d\n", burstDelayPerCycleInMs);

    // send messages until total count reached
    while (messagesSent < totalMessages)
    {
        int j = 0;
        long long startSeconds;
        long long endSeconds;
        startSeconds = timeInMilliseconds();

        while (j < messagesPerSecond && messagesSent < totalMessages) {
            printf("%d_%lld_%s\n", idCounter, startSeconds, data);
            idCounter++;
            j++;
            messagesSent++;
        }

        endSeconds = timeInMilliseconds();

        if (messagesSent < totalMessages)
        {
            msleep(timeBetweenCycleInMs + burstDelayPerCycleInMs + startSeconds - endSeconds);
        }
    }

    return 0;
}