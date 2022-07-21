/****************************************************************************
 *          TRQ_LIST.C
 *
 *          List messages in tr message's queue.
 *
 *          Copyright (c) 2019 Niyamaka.
 *          All Rights Reserved.
 ****************************************************************************/
#include <stdio.h>
#include <argp.h>
#include <time.h>
#include <errno.h>
#include <regex.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <ghelpers.h>

/***************************************************************************
 *              Constants
 ***************************************************************************/
#define NAME        "trq_list"
#define DOC         "List messages in tr message's queue."

#define VERSION     __ghelpers_version__
#define SUPPORT     "<niyamaka at yuneta.io>"
#define DATETIME    __DATE__ " " __TIME__

#define MEM_MIN_BLOCK           512
#define MEM_MAX_BLOCK           209715200   // 200*M
#define MEM_SUPERBLOCK          209715200   // 200*M
#define MEM_MAX_SYSTEM_MEMORY   4294967296  // 4*G

/***************************************************************************
 *              Structures
 ***************************************************************************/
/*
 *  Used by main to communicate with parse_opt.
 */
#define MIN_ARGS 0
#define MAX_ARGS 0
struct arguments
{
    char *args[MAX_ARGS+1];     /* positional args */
    int verbose;                /* verbose */
    int all;                    /* all message*/
    char *database;
    char *topic;
    char *from_t;
    char *to_t;
    char *key;
};

/***************************************************************************
 *              Prototypes
 ***************************************************************************/
static error_t parse_opt (int key, char *arg, struct argp_state *state);

/***************************************************************************
 *      Data
 ***************************************************************************/
const char *argp_program_version = NAME " " VERSION;
const char *argp_program_bug_address = SUPPORT;

/* Program documentation. */
static char doc[] = DOC;

/* A description of the arguments we accept. */
static char args_doc[] = "";

/*
 *  The options we understand.
 *  See https://www.gnu.org/software/libc/manual/html_node/Argp-Option-Vectors.html
 */
static struct argp_option options[] = {
/*-name-------------key-----arg---------flags---doc-----------------group */
{"verbose",         'l',    "LEVEL",    0,      "Verbose level (0=total, 1=metadata, 2=metadata+path, 3=metadata+record)", 3},
{"full",            'f',    0,          0,      "List full messages, not only pending."},
{0,                 0,      0,          0,      "Database keys",    4},
{"database",        'a',    "STRING",   0,      "Database",         4},
{"topic",           'b',    "STRING",   0,      "Topic",            4},
{"from",            1,      "STRING",   0,      "From rowid.",      4},
{"to",              2,      "STRING",   0,      "To rowid.",        4},
{"key",             3,      "STRING",   0,      "Key.",             4},
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
    case 'l':
        if(arg) {
            arguments->verbose = atoi(arg);
        }
        break;

    case 'f':
        arguments->all = 1;
        break;

    case 'a':
        arguments->database = arg;
        break;
    case 'b':
        arguments->topic = arg;
        break;
    case 1:
        arguments->from_t = arg;
        break;
    case 2:
        arguments->to_t = arg;
        break;
    case 3:
        arguments->key = arg;
        break;

    case ARGP_KEY_ARG:
        if (state->arg_num >= MAX_ARGS) {
            /* Too many arguments. */
            argp_usage (state);
        }
        arguments->args[state->arg_num] = arg;
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
 *
 ***************************************************************************/
PRIVATE int list_queue_msgs(
    const char *database,
    const char *topic_name,
    uint64_t from_t,
    uint64_t to_t,
    const char *key,
    int all,
    int verbose)
{
    json_t *tranger = tranger_startup(
        json_pack("{s:s, s:b}",
            "path", database,
            "master", 0
        )
    );
    if(!tranger) {
        exit(-1);
    }

    tr_queue trq = trq_open(
        tranger,
        topic_name,
        0,
        0,
        0,
        0
    );
    if(!trq) {
        exit(-1);
    }
    if(all) {
        trq_load_all(trq, key, from_t, to_t);
    } else {
        trq_load(trq);
    }

    char title[1024];
    int counter = 0;
    q_msg msg;
    qmsg_foreach_forward(trq, msg) {
        counter++;
        md_record_t md_record = trq_msg_md_record(msg);
        print_md1_record(trq_tranger(trq), trq_topic(trq), &md_record, title, sizeof(title));

        if(verbose) {
            if(verbose == 1) {
                printf("%s\n", title);
            }
            if(verbose == 2) {
                print_md2_record(trq_tranger(trq), trq_topic(trq), &md_record, title, sizeof(title));
                printf("%s\n", title);
            }
            if(verbose == 3) {
                json_t *jn_msg = trq_msg_json(msg);
                print_json2(title, jn_msg);
            }
        }
    }

    printf("Total: %d records\n\n", counter);

    trq_close(trq);
    tranger_shutdown(tranger);
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
    arguments.verbose = 0;
    arguments.all = 0;
    arguments.database = 0;
    arguments.topic = 0;
    arguments.from_t = 0;
    arguments.to_t = 0;
    arguments.key = 0;

    /*
     *  Parse arguments
     */
    argp_parse(&argp, argc, argv, 0, 0, &arguments);
    if(empty_string(arguments.database)) {
        fprintf(stderr, "What queue database?\n");
        exit(-1);
    }
    if(empty_string(arguments.topic)) {
        fprintf(stderr, "What queue topic?\n");
        exit(-1);
    }
    uint64_t from_t = 0;
    uint64_t to_t = 0;
    if(!empty_string(arguments.from_t)) {
        from_t = atoll(arguments.from_t);
    }
    if(!empty_string(arguments.to_t)) {
        to_t = atoll(arguments.to_t);
    }

    gbmem_startup_system(
        MEM_MAX_BLOCK,
        (INT_MAX < MEM_MAX_SYSTEM_MEMORY)? INT_MAX:MEM_MAX_SYSTEM_MEMORY
    );
//     gbmem_startup( /* Create memory core */
//         MEM_MIN_BLOCK,
//         MEM_MAX_BLOCK,
//         MEM_SUPERBLOCK,
//         MEM_MAX_SYSTEM_MEMORY,
//         NULL,               /* system memory functions */
//         0
//     );
    json_set_alloc_funcs(
        gbmem_malloc,
        gbmem_free
    );

    log_startup(
        "test",             // application name
        VERSION,            // applicacion version
        "test_glogger"     // executable program, to can trace stack
    );
    log_add_handler("test_stdout", "stdout", LOG_OPT_LOGGER, 0);

    /*
     *  Do your work
     */
    return list_queue_msgs(
        arguments.database,
        arguments.topic,
        from_t,
        to_t,
        arguments.key,
        arguments.all,
        arguments.verbose
    );
}
