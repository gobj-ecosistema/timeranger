/****************************************************************************
 *          TRANGER_LIST.C
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
#define NAME        "trmsg_list"
#define DOC         "List messages of TrTb database."

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
{"verbose",             'l',    "LEVEL",            0,      "Verbose level (0=total, 1=active, 2=active+metadata, 3=instances, 4=instances+metadata)", 3},
{"mode",                'm',    "MODE",             0,      "Mode: form or table", 3},
{"fields",              'f',    "FIELDS",           0,      "Print only this fields", 3},

{0,                     0,      0,                  0,      "Search conditions", 4},
{"from-t",              1,      "TIME",             0,      "From time.",       4},
{"to-t",                2,      "TIME",             0,      "To time.",         4},
{"from-rowid",          4,      "TIME",             0,      "From rowid.",      5},
{"to-rowid",            5,      "TIME",             0,      "To rowid.",        5},

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
    char *fullpath,         // directory+filename found
    const char *directory,  // directory of found filename
    char *name,             // dname[255]
    int level,              // level of tree where file found
    int index               // index of file inside of directory, relative to 0
)
{
    char *p = strrchr(directory, '/');
    if(p) {
        printf("  %s\n", p+1);
    } else {
        printf("  %s\n", directory);
    }
    return TRUE; // to continue
}

PRIVATE int list_databases(const char *path)
{
    printf("Databases found:\n");
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
        printf("  %s\n", p+1);
    } else {
        printf("  %s\n", directory);
    }
    return TRUE; // to continue
}

PRIVATE int list_topics(const char *path, const char *database)
{
    char temp[1024];
    snprintf(temp, sizeof(temp), "%s/%s", path, database);
    printf("Topics found:\n");
    walk_dir_tree(
        temp,
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
PRIVATE int list_active_callback(
    json_t *list,  // Not yours!
    const char *sub,
    json_t *record, // It's yours, Must be owned
    void *user_data1,
    void *user_data2
)
{
    static BOOL first_time = TRUE;
    total_counter++;
    partial_counter++;
    int verbose = kw_get_int(list, "verbose", 0, KW_REQUIRED);

    BOOL table_mode = FALSE;
    if(!empty_string(arguments.mode) || !empty_string(arguments.fields)) {
        table_mode = TRUE;
    }

    if(verbose == 0) {
        JSON_DECREF(record);
        return 0;
    }

    if(table_mode) {
        if(!empty_string(arguments.fields)) {
            const char ** keys = 0;
            keys = split2(arguments.fields, ", ", 0);
            json_t *jn_record_with_fields = kw_clone_by_path(
                record,   // owned
                keys
            );
            split_free2(keys);
            record = jn_record_with_fields;

        }
        if(json_object_size(record)>0) {
            const char *key;
            json_t *jn_value;
            int len;
            int col;
            if(first_time) {
                first_time = FALSE;
                col = 0;
                json_object_foreach(record, key, jn_value) {
                    len = strlen(key);
                    if(col == 0) {
                        printf("%*.*s", len, len, key);
                    } else {
                        printf(" %*.*s", len, len, key);
                    }
                    col++;
                }
                printf("\n");
                col = 0;
                json_object_foreach(record, key, jn_value) {
                    len = strlen(key);
                    if(col == 0) {
                        printf("%*.*s", len, len, "=======================================");
                    } else {
                        printf(" %*.*s", len, len, "=======================================");
                    }
                    col++;
                }
                printf("\n");
            }
            col = 0;

            json_object_foreach(record, key, jn_value) {
                char *s = json2uglystr(jn_value);
                if(col == 0) {
                    printf("%s", s);
                } else {
                    printf(" %s", s);
                }
                gbmem_free(s);
                col++;
            }
            printf("\n");
        }

    } else {
        print_json2(sub, record);
    }

    JSON_DECREF(record);
    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int list_instances_callback(
    json_t *list,  // Not yours!
    const char *sub,
    json_t *instances, // It's yours, Must be owned
    void *user_data1,
    void *user_data2
)
{
    static BOOL first_time = TRUE;
    total_counter++;
    partial_counter++;
    int verbose = kw_get_int(list, "verbose", 0, KW_REQUIRED);

    BOOL table_mode = FALSE;
    if(!empty_string(arguments.mode) || !empty_string(arguments.fields)) {
        table_mode = TRUE;
    }

    if(verbose == 0) {
        JSON_DECREF(instances);
        return 0;
    }

    if(table_mode) {
        if(!empty_string(arguments.fields)) {
            const char ** keys = 0;
            keys = split2(arguments.fields, ", ", 0);
            json_t *jn_record_with_fields = kw_clone_by_path(
                instances,   // owned
                keys
            );
            split_free2(keys);
            instances = jn_record_with_fields;

        }
        // TODO es un array capullo! no un dict.
        if(json_object_size(instances)>0) {
            const char *key;
            json_t *jn_value;
            int len;
            int col;
            if(first_time) {
                first_time = FALSE;
                col = 0;
                json_object_foreach(instances, key, jn_value) {
                    len = strlen(key);
                    if(col == 0) {
                        printf("%*.*s", len, len, key);
                    } else {
                        printf(" %*.*s", len, len, key);
                    }
                    col++;
                }
                printf("\n");
                col = 0;
                json_object_foreach(instances, key, jn_value) {
                    len = strlen(key);
                    if(col == 0) {
                        printf("%*.*s", len, len, "=======================================");
                    } else {
                        printf(" %*.*s", len, len, "=======================================");
                    }
                    col++;
                }
                printf("\n");
            }
            col = 0;

            json_object_foreach(instances, key, jn_value) {
                char *s = json2uglystr(jn_value);
                if(col == 0) {
                    printf("%s", s);
                } else {
                    printf(" %s", s);
                }
                gbmem_free(s);
                col++;
            }
            printf("\n");
        }

    } else {
        print_json2(sub, instances);
    }

    JSON_DECREF(instances);
    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int _list_messages(
    char *path,
    char *database,
    char *topic_name,
    json_t *jn_filter,
    int verbose)
{
    /*-------------------------------*
     *  Startup TrTb
     *-------------------------------*/
    json_t *jn_tranger = json_pack("{s:s, s:s}",
        "path", path,
        "database", database
    );
    json_t * tranger = tranger_startup(jn_tranger);
    if(!tranger) {
        fprintf(stderr, "Can't startup tranger %s/%s\n\n", path, database);
        exit(-1);
    }

    /*-------------------------------*
     *  Open topic
     *-------------------------------*/
    JSON_INCREF(jn_filter);
    json_t *list = trmsg_open_list(
        tranger,
        topic_name,
        jn_filter
    );
    if(list) {
        kw_set_dict_value(list, "verbose", json_integer(verbose));
        if(verbose <3) {
            trmsg_foreach_active_records(
                list,
                verbose==2?TRUE:FALSE, // with_metadata
                list_active_callback,
                0,
                0
            );
        } else {
            trmsg_foreach_instances_records(
                list,
                verbose==4?TRUE:FALSE, // with_metadata
                list_instances_callback,
                0,
                0
            );
        }

        trmsg_close_list(tranger, list);
    }

    /*-------------------------------*
     *  Free resources
     *-------------------------------*/
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
    snprintf(bftemp, sizeof(bftemp), "%s%s%s",
        path,
        (path[strlen(path)-1]=='/')?"":"/",
        "topic_desc.json"
    );
    if(is_regular_file(bftemp)) {
        pop_last_segment(bftemp); // pop topic_desc.json
        topic = pop_last_segment(bftemp);
        database = pop_last_segment(bftemp);
        if(!empty_string(topic)) {
            return _list_messages(
                bftemp,
                database,
                topic,
                match_cond,
                verbose
            );
        }
    }

    if(empty_string(database)) {
        fprintf(stderr, "What Database?\n\n");
        list_databases(path);
        exit(-1);
    }

    if(empty_string(topic)) {
        fprintf(stderr, "What Topic?\n\n");
        list_topics(path, database);
        exit(-1);
    }

    return _list_messages(
        path,
        database,
        topic,
        match_cond,
        verbose
    );
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE BOOL list_recursive_topic_cb(
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
    list_params_t list_params2 = *list_params;

    char *p = strrchr(directory, '/');
    if(p) {
        snprintf(list_params2.topic, sizeof(list_params2.topic), "%s", p+1);
    } else {
        snprintf(list_params2.topic, sizeof(list_params2.topic), "%s", directory);
    }
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

PRIVATE int list_recursive_topics(list_params_t *list_params)
{
    char temp[1*1024];
    snprintf(temp, sizeof(temp), "%s%s%s",
        list_params->path,
        list_params->path[strlen(list_params->path)-1]=='/'?"":"/",
        list_params->database
    );

    walk_dir_tree(
        temp,
        "topic_desc.json",
        WD_RECURSIVE|WD_MATCH_REGULAR_FILE,
        list_recursive_topic_cb,
        list_params
    );

    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE BOOL list_recursive_db_cb(
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
    list_params_t list_params2 = *list_params;

    snprintf(list_params2.path, sizeof(list_params2.path), "%s", directory);

    if(empty_string(list_params2.topic)) {
        list_recursive_topics(&list_params2);
    } else {
        partial_counter = 0;
        _list_messages(
            list_params2.path,
            list_params2.database,
            list_params2.topic,
            list_params2.match_cond,
            list_params2.verbose
        );
        printf("\n====> %s: %d records\n", directory, partial_counter);
    }

    return TRUE; // to continue
}

PRIVATE int list_recursive_databases(list_params_t *list_params)
{
    walk_dir_tree(
        list_params->path,
        "__timeranger__.json",
        WD_RECURSIVE|WD_MATCH_REGULAR_FILE,
        list_recursive_db_cb,
        list_params
    );

    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int list_recursive_gps_msg(
    char *path,
    char *database,
    char *topic,
    json_t *match_cond,
    int verbose)
{
    list_params_t list_params;
    memset(&list_params, 0, sizeof(list_params));

    snprintf(list_params.path, sizeof(list_params.path), "%s", path);
    if(database) {
        snprintf(list_params.database, sizeof(list_params.database), "%s", database);
    }
    if(topic) {
        snprintf(list_params.topic, sizeof(list_params.topic), "%s", topic);
    }
    list_params.match_cond = match_cond;
    list_params.verbose = verbose;

    if(empty_string(database)) {
        return list_recursive_databases(&list_params);
    }

    if(empty_string(topic)) {
        return list_recursive_topics(&list_params);
    }

    return list_messages(
        path,
        database,
        topic,
        match_cond,
        verbose
    );
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

    if(json_object_size(match_cond)>0) {
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
        fprintf(stderr, "What TrTb path?\n");
        exit(-1);
    }
    if(arguments.recursive) {
        list_recursive_gps_msg(
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
