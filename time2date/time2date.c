/****************************************************************************
 *          TIME2DATE.C
 *
 *          Conver time_t to date
 *
 *          Copyright (c) 2018 Niyamaka.
 *          All Rights Reserved.
 ****************************************************************************/
#include <stdio.h>
#include <argp.h>
#include <time.h>
#include <errno.h>
#include <pcre2posix.h>
#include <locale.h>
#include <libintl.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <ghelpers.h>

/***************************************************************************
 *              Constants
 ***************************************************************************/
#define NAME        "time2date"
#define DOC         "Convert time_t to ascii date and viceversa"

#define VERSION     "1.0.0"
#define SUPPORT     "<niyamaka at yuneta.io>"
#define DATETIME    __DATE__ " " __TIME__


/***************************************************************************
 *              Structures
 ***************************************************************************/
/*
 *  Used by main to communicate with parse_opt.
 */
#define MIN_ARGS 0
#define MAX_ARGS 1
struct arguments
{
    char *args[MAX_ARGS+1];     /* positional args */
    char *TZ;
};

/***************************************************************************
 *              Prototypes
 ***************************************************************************/
static error_t parse_opt (int key, char *arg, struct argp_state *state);

/***************************************************************************
 *      Data
 ***************************************************************************/
int counter = 0;
const char *argp_program_version = NAME " " VERSION;
const char *argp_program_bug_address = SUPPORT;

/* Program documentation. */
static char doc[] = DOC;

/* A description of the arguments we accept. */
static char args_doc[] = "TIME_T";

/*
 *  The options we understand.
 *  See https://www.gnu.org/software/libc/manual/html_node/Argp-Option-Vectors.html
 */
static struct argp_option options[] = {
/*-name-----------------key-----arg-----------------flags---doc-----------------group */
{"TZ",                  'z',    "TIME_ZONE",        0,      "Time zone.",       4},
{0}
};

/* Our argp parser. */
static struct argp argp = {
    options,
    parse_opt,
    args_doc,
    doc
};

/***************************************************************************
 *  Parse a single option
 ***************************************************************************/
static error_t parse_opt (int key, char *arg, struct argp_state *state)
{
    /*
     *  Get the input argument from argp_parse,
     *  which we know is a pointer to our arguments structure.
     */
    struct arguments *arguments = state->input;

    switch (key) {
    case ARGP_KEY_ARG:
        if (state->arg_num >= MAX_ARGS) {
            /* Too many arguments. */
            argp_usage (state);
        }
        arguments->args[state->arg_num] = arg;
        break;

    case 'z':
        arguments->TZ = arg;
        break;

    case ARGP_KEY_END:
        if (state->arg_num < MIN_ARGS) {
            /* Not enough arguments. */
            argp_usage (state);
        }
        break;

    default:
        return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

/***************************************************************************
 *                      Main
 ***************************************************************************/
int main(int argc, char *argv[])
{
    struct arguments arguments;

    /*
     *  Default values
     */
    memset(&arguments, 0, sizeof(arguments));

    /*
     *  Parse arguments
     */
    argp_parse(&argp, argc, argv, 0, 0, &arguments);

    timestamp_t timestamp;
    int offset;

    char now[32];
    if(arguments.args[0]==0) {
        time_t t;
        time(&t);
        snprintf(now, sizeof(now), "%ld", t);
        arguments.args[0] = now;
    }
    if(all_numbers(arguments.args[0])) {
        timestamp = atoll(arguments.args[0]);
    } else {
        parse_date_basic(arguments.args[0], &timestamp, &offset);
    }

    setlocale(LC_ALL, "en_US.UTF-8"); //Deja el numero (PRItime) tal cual

    struct tm *tm;
    char utc_stamp[164];

    tm = gmtime((time_t *)&timestamp);
    strftime(utc_stamp, sizeof(utc_stamp), "%Y-%m-%dT%H:%M:%S%z", tm);

    if(empty_string(arguments.TZ)) {
        char local_stamp[164];
        tm = localtime((time_t *)&timestamp);
        strftime(local_stamp, sizeof(local_stamp), "%Y-%m-%dT%H:%M:%S%z", tm);

        printf("gmtime of %"PRItime": UTC %s, LOCAL %s, yday %d\n",
            timestamp, utc_stamp, local_stamp, tm->tm_yday
        );
    } else {
        char local_stamp[164];
        tm = localtime((time_t *)&timestamp);
        strftime(local_stamp, sizeof(local_stamp), "%Y-%m-%dT%H:%M:%S%z", tm);

        char tz_stamp[164];
        time_t offset;
        struct tm tm2;
        gmtime2timezone(timestamp, arguments.TZ, &tm2, &offset);
        strftime(tz_stamp, sizeof(tz_stamp), "%FT%T%z", &tm2);

        printf("gmtime of %"PRItime": UTC %s, %s %s, LOCAL %s, yday %d\n",
            timestamp, utc_stamp, arguments.TZ, tz_stamp, local_stamp, tm2.tm_yday
        );
    }
    return 0;
}
