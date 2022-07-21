/****************************************************************************
 *          TRMSG_LIST.C
 *
 *  List Messages (ordered by pkey: active and their instances) with TimeRanger
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
#define DOC         "List messages (ordered by pkey: active and their instances) with TimeRanger database."

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
{"recursive",           'r',    0,                  0,      "List recursively.",  2},

{0,                     0,      0,                  0,      "Presentation",     3},
{"verbose",             'l',    "LEVEL",            0,      "Verbose level (0=total, 1=active, 2=instances, 3=message(active+instances), 4=message(active+ #of instances), 5=message(active+ #of instances+ #field_count))", 3},
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

    if(verbose == 0 && !table_mode) {
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
PRIVATE int _list_messages(list_params_t *list_params)
{
    char *path = list_params->arguments->path;
    char *database = list_params->arguments->database;
    char *topic_name = list_params->arguments->topic;
    int verbose = list_params->arguments->verbose;
    json_t *match_cond = list_params->match_cond;

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
    JSON_INCREF(match_cond);
    json_t *list = trmsg_open_list(
        tranger,
        topic_name,
        match_cond
    );
    if(list) {
        //0=total, 1=active, 2=instances, 3=message(active+instances), 4=message(active+ #of instances), 5=message(active+ #of instances+ #field_count)
        kw_set_dict_value(list, "verbose", json_integer(verbose));
        switch(verbose) {
        case 1:
            trmsg_foreach_active_messages(
                list,
                list_active_callback,
                0,
                0,
                0
            );
            break;
        case 2:
            trmsg_foreach_instances_messages(
                list,
                list_instances_callback,
                0,
                0,
                0
            );
            break;
        case 3:
            trmsg_foreach_messages(
                list,
                FALSE,
                list_instances_callback,
                0,
                0,
                0
            );
            break;
        case 4:
            {
                json_int_t total = 0;
                json_t *messages = trmsg_get_messages(list);
                const char *key;
                json_t *message;
                json_object_foreach(messages, key, message) {
                    json_t *instances = json_object_get(message, "instances");
                    json_int_t n = json_array_size(instances);
                    printf("Key: %s, instances: %lld\n", key, n);
                    total += n;
                }
                printf("Total instances: %lld\n", total);
            }
            break;
        case 5:
            {
                int list_size;
                const char **fields = split2(list_params->arguments->fields, ",", &list_size);

                json_int_t total = 0;
                json_t *messages = trmsg_get_messages(list);
                const char *key;
                json_t *message;
                json_object_foreach(messages, key, message) {
                    json_t *instances = json_object_get(message, "instances");
                    json_int_t n = json_array_size(instances);
                    printf("Key: %s, instances: %lld\n", key, n);
                    total += n;

                    json_t *jn_dict = json_object();
                    for(int i=0; i<list_size; i++) {
                        const char *field = *(fields +i);
                        json_t *x = json_object();
                        json_object_set_new(jn_dict, field, x);

                        int idx; json_t *instance;
                        json_array_foreach(instances, idx, instance) {
                            json_t *v = kw_get_dict_value(instance, field, 0, 0);
                            if(v) {
                                char *s = jn2string(v);
                                json_int_t z = kw_get_int(x, s, 0, KW_CREATE);
                                z++;
                                json_object_set_new(x, s, json_integer(z));
                                gbmem_free(s);
                            }
                        }
                    }
                    print_json2("", jn_dict);
                    printf("\n");
                    json_decref(jn_dict);
                }
                printf("Total instances: %lld\n", total);
                split_free2(fields);
            }
            break;
        default:
            break;
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
PRIVATE int list_topic_messages(list_params_t *list_params)
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
    return _list_messages(list_params);
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

    partial_counter = 0;
    pop_last_segment(fullpath);
    list_params->arguments->topic = pop_last_segment(fullpath);
    list_params->arguments->database = pop_last_segment(fullpath);
    list_params->arguments->path = fullpath;
    _list_messages(list_params);

    printf("====> %s %s: %d records\n\n",
        list_params->arguments->database,
        list_params->arguments->topic,
        partial_counter
    );

    return TRUE; // to continue
}

PRIVATE int list_recursive_topics(list_params_t *list_params)
{
    walk_dir_tree(
        list_params->arguments->path,
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
PRIVATE int list_recursive_topic_messages(list_params_t *list_params)
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
            return _list_messages(list_params);
        }

        fprintf(stderr, "What Database?\n\nFound:\n\n");
        list_databases(list_params->arguments->path);
        exit(-1);
    }

    list_params->arguments->topic = "";
    list_params->arguments->database = "";
    list_params->arguments->path = path_tranger;
    return list_recursive_topics(list_params);
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
        if(all_numbers(arguments.from_t)) {
            timestamp = atoll(arguments.from_t);
        } else {
            timestamp = approxidate(arguments.from_t);
        }
        json_object_set_new(match_cond, "from_t", json_integer(timestamp));
    }
    if(arguments.to_t) {
        timestamp_t timestamp;
        if(all_numbers(arguments.to_t)) {
            timestamp = atoll(arguments.to_t);
        } else {
            timestamp = approxidate(arguments.to_t);
        }
        json_object_set_new(match_cond, "to_t", json_integer(timestamp));
    }
    if(arguments.from_tm) {
        timestamp_t timestamp;
        if(all_numbers(arguments.from_tm)) {
            timestamp = atoll(arguments.from_tm);
        } else {
            timestamp = approxidate(arguments.from_tm);
        }
        json_object_set_new(match_cond, "from_tm", json_integer(timestamp));
    }
    if(arguments.to_tm) {
        timestamp_t timestamp;
        if(all_numbers(arguments.to_tm)) {
            timestamp = atoll(arguments.to_tm);
        } else {
            timestamp = approxidate(arguments.to_tm);
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
        fprintf(stderr, "What TimeRanger path?\n");
        fprintf(stderr, "You must supply --path option\n\n");
        exit(-1);
    }

    list_params_t list_params;
    memset(&list_params, 0, sizeof(list_params));
    list_params.arguments = &arguments;
    list_params.match_cond = match_cond;

    if(arguments.recursive) {
        list_recursive_topic_messages(&list_params);
    } else {
        list_topic_messages(&list_params);
    }

    JSON_DECREF(match_cond);

    clock_gettime (CLOCK_MONOTONIC, &et);

    /*-------------------------------------*
     *  Print times
     *-------------------------------------*/
    dt = ts_diff2(st, et);

    setlocale(LC_ALL, "");
    printf("====> Total: %'d records; %'f seconds; %'lu op/sec\n\n",
        total_counter,
        dt,
        (unsigned long)(((double)total_counter)/dt)
    );

    gbmem_shutdown();
    return 0;
}
