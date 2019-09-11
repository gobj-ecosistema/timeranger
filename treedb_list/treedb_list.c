/****************************************************************************
 *          TREEDB_LIST.C
 *
 *          List messages of tranger database
 *
 *          Copyright (c) 2018 Niyamaka.
 *          All Rights Reserved.
 ****************************************************************************/
#include <stdio.h>
#include <argp.h>
#include <time.h>
#include <errno.h>
#include <regex.h>
#include <locale.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <ghelpers.h>

/***************************************************************************
 *              Constants
 ***************************************************************************/
#define NAME        "treedb_list"
#define DOC         "List messages of Treedb database."

#define VERSION     __ghelpers_version__
#define SUPPORT     "<niyamaka at yuneta.io>"
#define DATETIME    __DATE__ " " __TIME__

/***************************************************************************
 *              Structures
 ***************************************************************************/
typedef struct {
    char path[256];
    char database[256];
    char topic[256];
    json_t *match_cond;
    int verbose;
} list_params_t;


/*
 *  Used by main to communicate with parse_opt.
 */
#define MIN_ARGS 0
#define MAX_ARGS 0
struct arguments
{
    char *args[MAX_ARGS+1];     /* positional args */

    char *path;
    char *database;
    char *topic;
    int recursive;
    char *mode;
    char *fields;
    int verbose;

    char *user_flag_mask_set;
    char *user_flag_mask_notset;

    char *system_flag_mask_set;
    char *system_flag_mask_notset;

    char *key;
    char *notkey;

    char *from_tm;
    char *to_tm;

};

/***************************************************************************
 *              Prototypes
 ***************************************************************************/
static error_t parse_opt (int key, char *arg, struct argp_state *state);

/***************************************************************************
 *      Data
 ***************************************************************************/
struct arguments arguments;
int total_counter = 0;
int partial_counter = 0;
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
/*-name-----------------key-----arg-----------------flags---doc-----------------group */
{0,                     0,      0,                  0,      "Database",         2},
{"path",                'a',    "PATH",             0,      "Path.",            2},
{"database",            'b',    "DATABASE",         0,      "Database.",        2},
{"topic",               'c',    "TOPIC",            0,      "Topic.",           2},
{"recursive",           'r',    0,                  0,      "List recursively.",  2},

{0,                     0,      0,                  0,      "Presentation",     3},
{"verbose",             'l',    "LEVEL",            0,      "Verbose level (0=total, 1=metadata, 2=metadata+path, 3=metadata+record)", 3},
{"mode",                'm',    "MODE",             0,      "Mode: form or table", 3},
{"fields",              'f',    "FIELDS",           0,      "Print only this fields", 3},

{0,                     0,      0,                  0,      "Search conditions", 4},
{"user-flag-set",       9,      "MASK",             0,      "Mask of User Flag set.",   6},
{"user-flag-not-set",   10,     "MASK",             0,      "Mask of User Flag not set.",6},

{"system-flag-set",     13,     "MASK",             0,      "Mask of System Flag set.",   7},
{"system-flag-not-set", 14,     "MASK",             0,      "Mask of System Flag not set.",7},

{"key",                 21,     "KEY",              0,      "Key.",             9},
{"not-key",             22,     "KEY",              0,      "Not key.",         9},

{"from-tm",             25,     "TIME",             0,      "From msg time.",       10},
{"to-tm",               26,     "TIME",             0,      "To msg time.",         10},

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
    case 'a':
        arguments->path= arg;
        break;
    case 'b':
        arguments->database= arg;
        break;
    case 'c':
        arguments->topic= arg;
        break;
    case 'r':
        arguments->recursive = 1;
        break;
    case 'l':
        if(arg) {
            arguments->verbose = atoi(arg);
        }
        break;
    case 'm':
        arguments->mode = arg;
        break;
    case 'f':
        arguments->fields = arg;
        break;

    case 9:
        arguments->user_flag_mask_set = arg;
        break;
    case 10:
        arguments->user_flag_mask_notset = arg;
        break;

    case 13:
        arguments->system_flag_mask_set = arg;
        break;
    case 14:
        arguments->system_flag_mask_notset = arg;
        break;

    case 21:
        arguments->key = arg;
        break;
    case 22:
        arguments->notkey = arg;
        break;

    case 25: // from_tm
        arguments->from_tm = arg;
        break;
    case 26: // to_tm
        arguments->to_tm = arg;
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
static inline double ts_diff2 (struct timespec start, struct timespec end)
{
    uint64_t s, e;
    s = ((uint64_t)start.tv_sec)*1000000 + ((uint64_t)start.tv_nsec)/1000;
    e = ((uint64_t)end.tv_sec)*1000000 + ((uint64_t)end.tv_nsec)/1000;
    return ((double)(e-s))/1000000;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE BOOL list_db_cb(
    void *user_data,
    wd_found_type type,     // type found
    const char *fullpath,   // directory+filename found
    const char *directory,  // directory of found filename
    const char *name,       // name of type found
    int level,              // level of tree where file found
    int index               // index of file inside of directory, relative to 0
)
{
    printf("  %s\n", name);
    return TRUE; // to continue
}

PRIVATE int list_databases(const char *path)
{
    printf("Databases found:\n");
    walk_dir_tree(
        path,
        ".*\\.treedb_schema\\.json",
        WD_RECURSIVE|WD_MATCH_REGULAR_FILE,
        list_db_cb,
        0
    );
    printf("\n");
    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int _list_messages(
    char *path,
    char *database,
    char *topic,
    json_t *match_cond,
    int verbose)
{
    /*-------------------------------*
     *  Startup TimeRanger
     *-------------------------------*/
    json_t *jn_tranger = json_pack("{s:s, s:s}",
        "path", path,
        "database", ""
    );
    json_t * tranger = tranger_startup(jn_tranger);
    if(!tranger) {
        fprintf(stderr, "Can't startup tranger %s/%s\n\n", path, database);
        exit(-1);
    }

    /*-------------------------------*
     *  Open treedb
     *-------------------------------*/
    treedb_open_db(
        tranger,  // owned
        database,
        0, // jn_schema_sample
        "persistent"
    );

    json_t *treedb = kw_get_subdict_value(tranger, "treedbs", database, 0, 0);
    const char *topic_name; json_t *topic_data;
    json_object_foreach(treedb, topic_name, topic_data) {
        if(!empty_string(topic)) {
            if(strcmp(topic, topic_name)!=0) {
                continue;
            }
        }
        json_t *node_list = treedb_list_nodes( // Return MUST be decref
            tranger,
            database,
            topic_name,
            0, // jn_ids,     // owned
            0  // jn_filter   // owned
        );

        print_json2(topic_name, node_list);

        total_counter += json_array_size(node_list);

        JSON_DECREF(node_list);
    }

    /*-------------------------------*
     *  Free resources
     *-------------------------------*/
    treedb_close_db(tranger, database);
    tranger_shutdown(tranger);

    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int list_messages(
    char *path,
    char *database,
    char *topic,
    json_t *match_cond,
    int verbose)
{
    /*
     *  Check if path contains all
     */
    char bftemp[PATH_MAX];
    if(!empty_string(database)) {
        snprintf(bftemp, sizeof(bftemp), "%s%s%s",
            path,
            (path[strlen(path)-1]=='/')?"":"/",
            database
        );

    } else {
        snprintf(bftemp, sizeof(bftemp), "%s%s",
            path,
            (path[strlen(path)-1]=='/')?"":""
        );
    }
    if(is_regular_file(bftemp)) {
        database = pop_last_segment(bftemp); // pop *.treedb_desc.json
        return _list_messages(
            bftemp,
            database,
            topic,
            match_cond,
            verbose
        );
    }

    fprintf(stderr, "What Database?\n\n");
    list_databases(path);
    exit(-1);
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE BOOL list_recursive_db_cb(
    void *user_data,
    wd_found_type type,     // type found
    const char *fullpath,   // directory+filename found
    const char *directory,  // directory of found filename
    const char *name,       // name of type found
    int level,              // level of tree where file found
    int index               // index of file inside of directory, relative to 0
)
{
    list_params_t *list_params = user_data;
    list_params_t list_params2 = *list_params;

    snprintf(list_params2.path, sizeof(list_params2.path), "%s", directory);
    snprintf(list_params2.database, sizeof(list_params2.database), "%s", name);

    partial_counter = 0;
    _list_messages(
        list_params2.path,
        list_params2.database,
        list_params2.topic,
        list_params2.match_cond,
        list_params2.verbose
    );
    printf("\n====> %s: %d records\n", directory, partial_counter);

    return TRUE; // to continue
}

PRIVATE int list_recursive_databases(list_params_t *list_params)
{
    walk_dir_tree(
        list_params->path,
        ".*\\.treedb_schema\\.json",
        WD_RECURSIVE|WD_MATCH_REGULAR_FILE,
        list_recursive_db_cb,
        list_params
    );

    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int list_recursive_msg(
    char *path,
    char *database,
    char *topic,
    json_t *match_cond,
    int verbose)
{
    list_params_t list_params;
    memset(&list_params, 0, sizeof(list_params));

    snprintf(list_params.path, sizeof(list_params.path), "%s", path);
    if(!empty_string(database)) {
        snprintf(list_params.database, sizeof(list_params.database), "%s", database);
    }
    if(!empty_string(topic)) {
        snprintf(list_params.topic, sizeof(list_params.topic), "%s", topic);
    }
    list_params.match_cond = match_cond;
    list_params.verbose = verbose;

    return list_recursive_databases(&list_params);
}

/***************************************************************************
 *                      Main
 ***************************************************************************/
int main(int argc, char *argv[])
{
    /*
     *  Default values
     */
    memset(&arguments, 0, sizeof(arguments));

    /*
     *  Parse arguments
     */
    argp_parse(&argp, argc, argv, 0, 0, &arguments);

    uint64_t MEM_MAX_SYSTEM_MEMORY = free_ram_in_kb() * 1024LL;
    MEM_MAX_SYSTEM_MEMORY /= 100LL;
    MEM_MAX_SYSTEM_MEMORY *= 90LL;  // Coge el 90% de la memoria

    uint64_t MEM_MAX_BLOCK = (MEM_MAX_SYSTEM_MEMORY / sizeof(md_record_t)) * sizeof(md_record_t);

    MEM_MAX_BLOCK = MIN(1*1024*1024*1024LL, MEM_MAX_BLOCK);  // 1*G max

    gbmem_startup_system(
        MEM_MAX_BLOCK,
        MEM_MAX_SYSTEM_MEMORY
    );
    json_set_alloc_funcs(
        gbmem_malloc,
        gbmem_free
    );

    log_startup(
        NAME,       // application name
        VERSION,    // applicacion version
        NAME        // executable program, to can trace stack
    );
    log_add_handler(NAME, "stdout", LOG_OPT_LOGGER, 0);


    /*----------------------------------*
     *  Match conditions
     *----------------------------------*/
    json_t *match_cond = json_object();


    if(arguments.user_flag_mask_set) {
        json_object_set_new(
            match_cond,
            "user_flag_mask_set",
            json_integer(atol(arguments.user_flag_mask_set))
        );
    }
    if(arguments.user_flag_mask_notset) {
        json_object_set_new(
            match_cond,
            "user_flag_mask_notset",
            json_integer(atol(arguments.user_flag_mask_notset))
        );
    }
    if(arguments.system_flag_mask_set) {
        json_object_set_new(
            match_cond,
            "system_flag_mask_set",
            json_integer(atol(arguments.system_flag_mask_set))
        );
    }
    if(arguments.system_flag_mask_notset) {
        json_object_set_new(
            match_cond,
            "system_flag_mask_notset",
            json_integer(atol(arguments.system_flag_mask_notset))
        );
    }
    if(arguments.key) {
        json_object_set_new(
            match_cond,
            "key",
            json_string(arguments.key)
        );
    }
    if(arguments.notkey) {
        json_object_set_new(
            match_cond,
            "notkey",
            json_string(arguments.notkey)
        );
    }

    if(json_object_size(match_cond)>0) {
        json_object_set_new(match_cond, "only_md", json_true());
    } else {
        JSON_DECREF(match_cond);
    }

    /*
     *  Do your work
     */
    struct timespec st, et;
    double dt;

    clock_gettime (CLOCK_MONOTONIC, &st);

    if(empty_string(arguments.path)) {
        fprintf(stderr, "What TimeRanger path?\n");
        exit(-1);
    }
    if(arguments.recursive) {
        list_recursive_msg(
            arguments.path,
            arguments.database,
            arguments.topic,
            match_cond,
            arguments.verbose
        );
    } else {
        list_messages(
            arguments.path,
            arguments.database,
            arguments.topic,
            match_cond,
            arguments.verbose
        );
    }
    JSON_DECREF(match_cond);

    clock_gettime (CLOCK_MONOTONIC, &et);

    /*-------------------------------------*
     *  Print times
     *-------------------------------------*/
    dt = ts_diff2(st, et);

    setlocale(LC_ALL, "");
    printf("\n====> Total: %'d records; %'f seconds; %'lu op/sec\n\n",
        total_counter,
        dt,
        (unsigned long)(((double)total_counter)/dt)
    );

    gbmem_shutdown();
    return 0;
}
