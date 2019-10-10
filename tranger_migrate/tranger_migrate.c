/****************************************************************************
 *          TRANGER_MIGRATE.C
 *
 *          Utility for migrate json to json records in a timeranger database.
 *
 *          Copyright (c) 2019 Niyamaka.
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
#include <libgen.h>
#include <ghelpers.h>

/***************************************************************************
 *              Constants
 ***************************************************************************/
#define NAME        "tranger_migrate"
#define DOC         "Migrate json to json records in a TimeRanger database."

#define VERSION     __ghelpers_version__
#define SUPPORT     "<niyamaka at yuneta.io>"
#define DATETIME    __DATE__ " " __TIME__

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

    char *path;
    char *database;
    char *topic;
    char *destination;
    int recursive;
    int verbose;

    char *from_t;
    char *to_t;

    char *from_rowid;
    char *to_rowid;

    char *user_flag_mask_set;
    char *user_flag_mask_notset;

    char *system_flag_mask_set;
    char *system_flag_mask_notset;

    char *key;
    char *notkey;

    char *from_tm;
    char *to_tm;

    char *change_pkey;
    char *new_pkey;
};

typedef struct {
    struct arguments *arguments;
    json_t *match_cond;
} list_params_t;

/***************************************************************************
 *              Prototypes
 ***************************************************************************/
static error_t parse_opt (int key, char *arg, struct argp_state *state);
PRIVATE int list_topics(const char *path);

/***************************************************************************
 *      Data
 ***************************************************************************/
struct arguments arguments;
int total_counter = 0;
int migrate_counter = 0;
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
{"path",                'a',    "PATH",             0,      "Path of database/topic.",2},
{"database",            'b',    "DATABASE",         0,      "Tranger database name.",2},
{"topic",               'c',    "TOPIC",            0,      "Topic name.",      2},
{"destination",         'd',    "DESTINATION",      0,      "Destination directory", 2},
{"recursive",           'r',    0,                  0,      "List recursively.",  2},
{"verbose",             'l',    "LEVEL",            0,      "Verbose level (0=total, 1=metadata, 2=metadata+path, 3=metadata+record.", 2},

{0,                     0,      0,                  0,      "Search conditions", 4},
{"from-t",              1,      "TIME",             0,      "From time.",       4},
{"to-t",                2,      "TIME",             0,      "To time.",         4},
{"from-rowid",          4,      "TIME",             0,      "From rowid.",      5},
{"to-rowid",            5,      "TIME",             0,      "To rowid.",        5},

{"user-flag-set",       9,      "MASK",             0,      "Mask of User Flag set.",   6},
{"user-flag-not-set",   10,     "MASK",             0,      "Mask of User Flag not set.",6},

{"system-flag-set",     13,     "MASK",             0,      "Mask of System Flag set.",   7},
{"system-flag-not-set", 14,     "MASK",             0,      "Mask of System Flag not set.",7},

{"key",                 15,     "KEY",              0,      "Key.",             9},
{"not-key",             16,     "KEY",              0,      "Not key.",         9},

{"from-tm",             17,     "TIME",             0,      "From msg time.",       10},
{"to-tm",               18,     "TIME",             0,      "To msg time.",         10},

{0,                     0,      0,                  0,      "Migrate primary key", 11},
{"change-pkey",         20,     "OLD-PKEY-NAME",    0,      "Name of old primary key", 11},
{"new-pkey",            21,     "NEW-PKEY-NAME",    0,      "Name of new primary key", 11},

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
    case 'd':
        arguments->destination= arg;
        break;
    case 'r':
        arguments->recursive = 1;
        break;
    case 'l':
        if(arg) {
            arguments->verbose = atoi(arg);
        }
        break;
    case 1: // from_t
        arguments->from_t = arg;
        break;
    case 2: // to_t
        arguments->to_t = arg;
        break;

    case 4: // from_rowid
        arguments->from_rowid = arg;
        break;
    case 5: // to_rowid
        arguments->to_rowid = arg;
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

    case 15:
        arguments->key = arg;
        break;
    case 16:
        arguments->notkey = arg;
        break;

    case 17: // from_tm
        arguments->from_tm = arg;
        break;
    case 18: // to_tm
        arguments->to_tm = arg;
        break;

    case 20: // OLD PKEY
        arguments->change_pkey = arg;
        break;
    case 21: // NEW PKEY
        arguments->new_pkey = arg;
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
    char *fullpath,         // directory+filename found
    const char *directory,  // directory of found filename
    char *name,             // dname[255]
    int level,              // level of tree where file found
    int index               // index of file inside of directory, relative to 0
)
{
    char *p = strrchr(fullpath, '/');
    if(p) {
        *p = 0;
    }
    char topic_path[PATH_MAX];
    snprintf(topic_path, sizeof(topic_path), "%s", fullpath);
    printf("TimeRanger ==> '%s'\n", fullpath);
    printf("  TimeRanger database: '%s'\n", pop_last_segment(fullpath));
    printf("  Path: '%s'\n", fullpath);
    list_topics(topic_path);

    return TRUE; // to continue
}

PRIVATE int list_databases(const char *path)
{
    walk_dir_tree(
        path,
        "__timeranger__.json",
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
PRIVATE BOOL list_topic_cb(
    void *user_data,
    wd_found_type type,     // type found
    char *fullpath,         // directory+filename found
    const char *directory,  // directory of found filename
    char *name,             // dname[255]
    int level,              // level of tree where file found
    int index               // index of file inside of directory, relative to 0
)
{
    char *p = strrchr(directory, '/');
    if(p) {
        printf("        %s\n", p+1);
    } else {
        printf("        %s\n", directory);
    }
    return TRUE; // to continue
}

PRIVATE int list_topics(const char *path)
{
    printf("    Topics:\n");
    walk_dir_tree(
        path,
        "topic_desc.json",
        WD_RECURSIVE|WD_MATCH_REGULAR_FILE,
        list_topic_cb,
        0
    );
    printf("\n");
    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int load_record_callback(
    json_t *tranger,
    json_t *topic,
    json_t *list,
    md_record_t *md_record,
    json_t *jn_record
)
{
    list_params_t *list_params = (list_params_t *)(size_t)kw_get_int(
        list, "list_params", 0, KW_REQUIRED
    );

    json_t *tranger_dst = kw_get_dict(list, "tranger_dst", 0, KW_REQUIRED);

    int verbose = list_params->arguments->verbose;

    char title[1024];
    print_md1_record(tranger, topic, md_record, title, sizeof(title));

    if(!jn_record) {
        jn_record = tranger_read_record_content(tranger, topic, md_record);
    }

    if(verbose == 1) {
        printf("%s\n", title);
    } else if(verbose == 2) {
        print_md2_record(tranger, topic, md_record, title, sizeof(title));
        printf("%s\n", title);
    } else if(verbose == 3) {
        print_json2(title, jn_record);
    }

    /*------------------------------------*
     *          Changes
     *------------------------------------*/
    if(!empty_string(list_params->arguments->change_pkey)) {
        //list_params->arguments->new_pkey
    }


    /*
     *  Save
     */
    md_record_t md_dst_record;
    int ret = tranger_append_record(
        tranger_dst,
        tranger_topic_name(topic),
        md_record->__t__,      // if 0 then the time will be set by TimeRanger with now time
        md_record->__user_flag__,
        &md_dst_record,  // required
        jn_record   // owned
    );
    if(ret == 0) {
        migrate_counter++;
        partial_counter++;
    }
    total_counter++;

    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int _migrate_messages(list_params_t *list_params)
{
    char *path_src = list_params->arguments->path;
    char *path_dst = list_params->arguments->destination;
    char *database = list_params->arguments->database;
    char *topic_name = list_params->arguments->topic;
    json_t *match_cond = list_params->match_cond;

    /*-------------------------------*
     *  Check not override
     *-------------------------------*/
    if(strncmp(path_src, path_dst, strlen(path_src))==0) {
        fprintf(stderr, "Can't override tranger: %s/%s\n\n", path_src, database);
        exit(-1);
    }

    /*-------------------------------*
     *  Startup TimeRanger source
     *-------------------------------*/
    json_t *jn_tranger_src = json_pack("{s:s, s:s}",
        "path", path_src,
        "database", database
    );
    json_t * tranger_src = tranger_startup(jn_tranger_src);
    if(!tranger_src) {
        fprintf(stderr, "Can't startup source tranger: %s/%s\n\n", path_src, database);
        exit(-1);
    }

    /*-----------------------------------*
     *  Startup TimeRanger destination
     *-----------------------------------*/
    json_t *jn_tranger_dst = json_pack("{s:s, s:s, s:b}",
        "path", path_dst,
        "database", database,
        "master", 1
    );
    json_object_set_new(jn_tranger_dst, "filename_mask",
        json_string(kw_get_str(tranger_src, "filename_mask", "", KW_REQUIRED))
    );
    json_object_set_new(jn_tranger_dst, "rpermission",
        json_integer(kw_get_int(tranger_src, "rpermission", 0, KW_REQUIRED))
    );
    json_object_set_new(jn_tranger_dst, "xpermission",
        json_integer(kw_get_int(tranger_src, "xpermission", 0, KW_REQUIRED))
    );

    json_t *tranger_dst = tranger_startup(jn_tranger_dst);
    if(!tranger_dst) {
        fprintf(stderr, "Can't startup destination tranger: %s/%s\n\n", path_dst, database);
        exit(-1);
    }

    /*-------------------------------*
     *  Open source topic
     *-------------------------------*/
    json_t *htopic_src = tranger_open_topic(
        tranger_src,
        topic_name,
        FALSE
    );
    if(!htopic_src) {
        fprintf(stderr, "Can't open source topic: %s\n\n", topic_name);
        exit(-1);
    }

    /*-------------------------------*
     *  Create destination topic
     *-------------------------------*/
    json_t *htopic_dst = tranger_open_topic(
        tranger_dst,
        topic_name,
        FALSE
    );
    if(htopic_dst) {
        fprintf(stderr, "Destination topic ALREADY exists: %s\n\n", topic_name);
        exit(-1);
    }

    /*------------------------------------*
     *          Changes
     *------------------------------------*/
    const char *pkey = kw_get_str(htopic_src, "pkey", "", KW_REQUIRED);
    const char *tkey = kw_get_str(htopic_src, "tkey", "", KW_REQUIRED);
    json_int_t system_flag = kw_get_int(htopic_src, "system_flag", 0, KW_REQUIRED);
    json_t *cols = kw_get_dict(htopic_src, "cols", 0, KW_REQUIRED);

    if(!empty_string(list_params->arguments->change_pkey)) {
        if(empty_string(list_params->arguments->new_pkey)) {
            fprintf(stderr, "What new pkey?\n\n");
            exit(-1);
        }
        pkey = list_params->arguments->new_pkey;
    }

    JSON_INCREF(cols);
    htopic_dst = tranger_create_topic(
        tranger_dst,
        topic_name,
        pkey,
        tkey,
        system_flag,
        cols // owned
    );

    if(!htopic_dst) {
        fprintf(stderr, "Can't create destination topic: %s\n\n", topic_name);
        exit(-1);
    }

    /*-------------------------------*
     *  Open source list
     *-------------------------------*/
    JSON_INCREF(match_cond);
    json_t *jn_list = json_pack("{s:s, s:o, s:I, s:I, s:O}",
        "topic_name", topic_name,
        "match_cond", match_cond?match_cond:json_object(),
        "load_record_callback", (json_int_t)(size_t)load_record_callback,
        "list_params", (json_int_t)(size_t)list_params,
        "tranger_dst", tranger_dst
    );

    json_t *tr_list = tranger_open_list(
        tranger_src,
        jn_list
    );
    tranger_close_list(tranger_src, tr_list);

    /*-------------------------------*
     *  Free resources
     *-------------------------------*/
    tranger_shutdown(tranger_src);
    tranger_shutdown(tranger_dst);

    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int migrate_topic_messages(list_params_t *list_params)
{
    char path_topic[PATH_MAX];

    build_path3(path_topic, sizeof(path_topic),
        list_params->arguments->path,
        list_params->arguments->database,
        list_params->arguments->topic
    );

    if(!file_exists(path_topic, "topic_desc.json")) {
        if(!is_directory(path_topic)) {
            fprintf(stderr, "Path not found: '%s'\n\n", path_topic);
            exit(-1);
        }
        fprintf(stderr, "What Database/Topic?\n\nFound:\n\n");
        list_databases(list_params->arguments->path);
        exit(-1);
    }
    list_params->arguments->topic = pop_last_segment(path_topic);
    list_params->arguments->database = pop_last_segment(path_topic);
    list_params->arguments->path = path_topic;
    return _migrate_messages(list_params);
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE BOOL migrate_recursive_topic_cb(
    void *user_data,
    wd_found_type type,     // type found
    char *fullpath,         // directory+filename found
    const char *directory,  // directory of found filename
    char *name,             // dname[255]
    int level,              // level of tree where file found
    int index               // index of file inside of directory, relative to 0
)
{
    list_params_t *list_params = user_data;

    partial_counter = 0;
    pop_last_segment(fullpath);
    list_params->arguments->topic = pop_last_segment(fullpath);
    list_params->arguments->database = pop_last_segment(fullpath);
    list_params->arguments->path = fullpath;
    _migrate_messages(list_params);

    printf("\n====> %s %s: %d records\n",
        list_params->arguments->database,
        list_params->arguments->topic,
        partial_counter
    );

    return TRUE; // to continue
}

PRIVATE int migrate_recursive_topics(list_params_t *list_params)
{
    walk_dir_tree(
        list_params->arguments->path,
        "topic_desc.json",
        WD_RECURSIVE|WD_MATCH_REGULAR_FILE,
        migrate_recursive_topic_cb,
        list_params
    );

    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int migrate_recursive_topic_messages(list_params_t *list_params)
{
    char path_tranger[PATH_MAX];

    build_path2(path_tranger, sizeof(path_tranger),
        list_params->arguments->path,
        list_params->arguments->database
    );

    if(!file_exists(path_tranger, "__timeranger__.json")) {
        if(!is_directory(path_tranger)) {
            fprintf(stderr, "Path not found: '%s'\n\n", path_tranger);
            exit(-1);
        }
        if(file_exists(path_tranger, "topic_desc.json")) {
            list_params->arguments->topic = pop_last_segment(path_tranger);
            list_params->arguments->database = pop_last_segment(path_tranger);
            list_params->arguments->path = path_tranger;
            return _migrate_messages(list_params);
        }

        fprintf(stderr, "What Database?\n\nFound:\n\n");
        list_databases(list_params->arguments->path);
        exit(-1);
    }

    list_params->arguments->topic = "";
    list_params->arguments->database = "";
    list_params->arguments->path = path_tranger;
    return migrate_recursive_topics(list_params);
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

    if(arguments.from_t) {
        timestamp_t timestamp;
        int offset;
        if(all_numbers(arguments.from_t)) {
            timestamp = atoll(arguments.from_t);
        } else {
            parse_date_basic(arguments.from_t, &timestamp, &offset);
        }
        json_object_set_new(match_cond, "from_t", json_integer(timestamp));
    }
    if(arguments.to_t) {
        timestamp_t timestamp;
        int offset;
        if(all_numbers(arguments.to_t)) {
            timestamp = atoll(arguments.to_t);
        } else {
            parse_date_basic(arguments.to_t, &timestamp, &offset);
        }
        json_object_set_new(match_cond, "to_t", json_integer(timestamp));
    }
    if(arguments.from_tm) {
        timestamp_t timestamp;
        int offset;
        if(all_numbers(arguments.from_tm)) {
            timestamp = atoll(arguments.from_tm);
        } else {
            parse_date_basic(arguments.from_tm, &timestamp, &offset);
        }
        json_object_set_new(match_cond, "from_tm", json_integer(timestamp));
    }
    if(arguments.to_tm) {
        timestamp_t timestamp;
        int offset;
        if(all_numbers(arguments.to_tm)) {
            timestamp = atoll(arguments.to_tm);
        } else {
            parse_date_basic(arguments.to_tm, &timestamp, &offset);
        }
        json_object_set_new(match_cond, "to_tm", json_integer(timestamp));
    }

    if(arguments.from_rowid) {
        json_object_set_new(match_cond, "from_rowid", json_integer(atoll(arguments.from_rowid)));
    }
    if(arguments.to_rowid) {
        json_object_set_new(match_cond, "to_rowid", json_integer(atoll(arguments.to_rowid)));
    }

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

    if(arguments.change_pkey) {
        json_object_set_new(
            match_cond,
            "change_pkey",
            json_string(arguments.change_pkey)
        );
    }

    if(json_object_size(match_cond)>0) {
        ;
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
        fprintf(stderr, "You must supply --path option\n\n");
        exit(-1);
    }
    if(empty_string(arguments.destination)) {
        fprintf(stderr, "What destination path?\n");
        fprintf(stderr, "You must supply --destination option\n\n");
        exit(-1);
    }
    char path_dst[PATH_MAX];
    realpath(arguments.destination, path_dst);
    arguments.destination = path_dst;
    if(!is_directory(arguments.destination)) {
        fprintf(stderr, "Destination path '%s' is not a directory\n\n", arguments.destination);
        exit(-1);
    }

    list_params_t list_params;
    memset(&list_params, 0, sizeof(list_params));
    list_params.arguments = &arguments;
    list_params.match_cond = match_cond;

    if(arguments.recursive) {
        migrate_recursive_topic_messages(&list_params);
    } else {
        migrate_topic_messages(&list_params);
    }

    JSON_DECREF(match_cond);

    clock_gettime (CLOCK_MONOTONIC, &et);

    /*-------------------------------------*
     *  Print times
     *-------------------------------------*/
    dt = ts_diff2(st, et);

    setlocale(LC_ALL, "");
    printf("\n====> Migrate: %'d records of %'d records; %'f seconds; %'lu op/sec\n\n",
        migrate_counter,
        total_counter,
        dt,
        (unsigned long)(((double)migrate_counter)/dt)
    );

    gbmem_shutdown();
    return 0;
}
