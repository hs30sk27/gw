#include "gw_storage.h"

#include "ui_crc16.h"
#include "ui_time.h"
#include "ui_types.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#if !defined(UI_USE_LITTLEFS)
# if defined(__has_include)
#  if __has_include("lfs.h")
#   define UI_USE_LITTLEFS (1)
#  else
#   define UI_USE_LITTLEFS (0)
#  endif
# else
#  define UI_USE_LITTLEFS (0)
# endif
#endif

#if (UI_USE_LITTLEFS == 1)
#include "lfs.h"
#endif

#ifndef UI_W25Q128_READ_SIZE
#define UI_W25Q128_READ_SIZE        (16u)
#endif
#ifndef UI_W25Q128_PROG_SIZE
#define UI_W25Q128_PROG_SIZE        (256u)
#endif
#ifndef UI_W25Q128_BLOCK_SIZE
#define UI_W25Q128_BLOCK_SIZE       (4096u)
#endif
#ifndef UI_W25Q128_BLOCK_COUNT
#define UI_W25Q128_BLOCK_COUNT      (4096u)
#endif
#ifndef UI_W25Q128_CACHE_SIZE
#define UI_W25Q128_CACHE_SIZE       (256u)
#endif
#ifndef UI_W25Q128_LOOKAHEAD_SIZE
#define UI_W25Q128_LOOKAHEAD_SIZE   (32u)
#endif
#ifndef UI_W25Q128_BLOCK_CYCLES
#define UI_W25Q128_BLOCK_CYCLES     (500u)
#endif
#ifndef GW_STORAGE_MAX_FILES
#define GW_STORAGE_MAX_FILES        (96u)
#endif
#ifndef GW_STORAGE_TCP_QUEUE_CAPACITY
#define GW_STORAGE_TCP_QUEUE_CAPACITY (24u * 3u)
#endif
#define GW_STORAGE_TCP_QUEUE_PATH   "/tcpq.idx"
#define GW_STORAGE_TCP_QUEUE_MAGIC  (0x54435131u) /* 'TCQ1' */
#define GW_STORAGE_TCP_QUEUE_VER    (1u)

#if (UI_USE_LITTLEFS == 1)
typedef struct __attribute__((packed))
{
    uint32_t magic;
    uint16_t version;
    uint16_t count;
    uint16_t head;
    uint16_t reserved;
    uint32_t drop_count;
} GW_StorageTcpQueueHdr_t;

typedef struct __attribute__((packed))
{
    GW_StorageTcpQueueHdr_t hdr;
    GW_StorageRecRef_t items[GW_STORAGE_TCP_QUEUE_CAPACITY];
} GW_StorageTcpQueueFile_t;

static lfs_t s_lfs;
static uint8_t s_lfs_read_buf[UI_W25Q128_CACHE_SIZE];
static uint8_t s_lfs_prog_buf[UI_W25Q128_CACHE_SIZE];
static uint8_t s_lfs_lookahead_buf[UI_W25Q128_LOOKAHEAD_SIZE];

static int prv_bd_read(const struct lfs_config *c,
                       lfs_block_t block,
                       lfs_off_t off,
                       void *buffer,
                       lfs_size_t size)
{
    uint32_t addr = ((uint32_t)block * (uint32_t)c->block_size) + (uint32_t)off;
    return GW_Storage_W25Q_Read(addr, buffer, (uint32_t)size);
}

static int prv_bd_prog(const struct lfs_config *c,
                       lfs_block_t block,
                       lfs_off_t off,
                       const void *buffer,
                       lfs_size_t size)
{
    uint32_t addr = ((uint32_t)block * (uint32_t)c->block_size) + (uint32_t)off;
    return GW_Storage_W25Q_Prog(addr, buffer, (uint32_t)size);
}

static int prv_bd_erase(const struct lfs_config *c, lfs_block_t block)
{
    uint32_t addr = (uint32_t)block * (uint32_t)c->block_size;
    (void)c;
    return GW_Storage_W25Q_Erase4K(addr);
}

static int prv_bd_sync(const struct lfs_config *c)
{
    (void)c;
    return GW_Storage_W25Q_Sync();
}

static const struct lfs_config s_lfs_cfg = {
    .read = prv_bd_read,
    .prog = prv_bd_prog,
    .erase = prv_bd_erase,
    .sync = prv_bd_sync,
    .read_size = UI_W25Q128_READ_SIZE,
    .prog_size = UI_W25Q128_PROG_SIZE,
    .block_size = UI_W25Q128_BLOCK_SIZE,
    .block_count = UI_W25Q128_BLOCK_COUNT,
    .block_cycles = UI_W25Q128_BLOCK_CYCLES,
    .cache_size = UI_W25Q128_CACHE_SIZE,
    .lookahead_size = UI_W25Q128_LOOKAHEAD_SIZE,
    .read_buffer = s_lfs_read_buf,
    .prog_buffer = s_lfs_prog_buf,
    .lookahead_buffer = s_lfs_lookahead_buf,
    .name_max = 64,
    .file_max = 0,
    .attr_max = 0,
};
#endif

__attribute__((weak)) bool GW_Storage_W25Q_PowerOn(void)
{
    return false;
}

__attribute__((weak)) void GW_Storage_W25Q_PowerDown(void)
{
}

__attribute__((weak)) int GW_Storage_W25Q_Read(uint32_t addr, void* buf, uint32_t size)
{
    (void)addr;
    (void)buf;
    (void)size;
    return -1;
}

__attribute__((weak)) int GW_Storage_W25Q_Prog(uint32_t addr, const void* buf, uint32_t size)
{
    (void)addr;
    (void)buf;
    (void)size;
    return -1;
}

__attribute__((weak)) int GW_Storage_W25Q_Erase4K(uint32_t addr)
{
    (void)addr;
    return -1;
}

__attribute__((weak)) int GW_Storage_W25Q_Sync(void)
{
    return 0;
}

static void prv_make_day_path(uint32_t epoch_sec, char* out, size_t out_sz)
{
    UI_DateTime_t dt;
    UI_Time_Epoch2016_ToCalendar(epoch_sec, &dt);
    (void)snprintf(out,
                   out_sz,
                   "/%04u%02u%02u.gwd",
                   (unsigned)dt.year,
                   (unsigned)dt.month,
                   (unsigned)dt.day);
}

static int32_t prv_day_index_from_name(const char* name)
{
    UI_DateTime_t dt;
    int y, m, d;

    if ((name == NULL) || (strlen(name) < 8u))
    {
        return -1;
    }

    if (sscanf(name, "%4d%2d%2d", &y, &m, &d) != 3)
    {
        return -1;
    }

    dt.year = (uint16_t)y;
    dt.month = (uint8_t)m;
    dt.day = (uint8_t)d;
    dt.hour = 0u;
    dt.min = 0u;
    dt.sec = 0u;
    dt.centi = 0u;
    return (int32_t)(UI_Time_Epoch2016_FromCalendar(&dt) / 86400u);
}

static int32_t prv_day_index_from_epoch(uint32_t epoch_sec)
{
    return (int32_t)(epoch_sec / 86400u);
}

static uint32_t prv_day_epoch_from_epoch(uint32_t epoch_sec)
{
    return (epoch_sec / 86400u) * 86400u;
}

#if (UI_USE_LITTLEFS == 1)
static bool prv_mount_fs(bool allow_format)
{
    int rc;

    if (!GW_Storage_W25Q_PowerOn())
    {
        return false;
    }

    rc = lfs_mount(&s_lfs, &s_lfs_cfg);
    if ((rc != 0) && allow_format)
    {
        rc = lfs_format(&s_lfs, &s_lfs_cfg);
        if (rc == 0)
        {
            rc = lfs_mount(&s_lfs, &s_lfs_cfg);
        }
    }
    if (rc != 0)
    {
        GW_Storage_W25Q_PowerDown();
        return false;
    }
    return true;
}

static void prv_unmount_fs(void)
{
    (void)lfs_unmount(&s_lfs);
    GW_Storage_W25Q_PowerDown();
}

static bool prv_is_gwd_name(const char* name)
{
    size_t n;
    if (name == NULL)
    {
        return false;
    }
    n = strlen(name);
    if (n < 5u)
    {
        return false;
    }
    return (strcmp(&name[n - 4u], ".gwd") == 0);
}

static int prv_cmp_file_desc(const void* a, const void* b)
{
    const GW_StorageFileInfo_t* fa = (const GW_StorageFileInfo_t*)a;
    const GW_StorageFileInfo_t* fb = (const GW_StorageFileInfo_t*)b;
    return strcmp(fb->name, fa->name); /* newest date file first */
}

static void prv_fill_file_bounds(GW_StorageFileInfo_t* info)
{
    char path[80];
    lfs_file_t file;
    GW_FileRec_t rec;

    if ((info == NULL) || (info->size < sizeof(GW_FileRec_t)) || (info->rec_count == 0u))
    {
        return;
    }

    (void)snprintf(path, sizeof(path), "/%s", info->name);
    if (lfs_file_open(&s_lfs, &file, path, LFS_O_RDONLY) != 0)
    {
        return;
    }

    if (lfs_file_read(&s_lfs, &file, &rec, sizeof(rec)) == (lfs_ssize_t)sizeof(rec))
    {
        info->first_epoch_sec = rec.epoch_sec;
    }
    if (info->size >= sizeof(rec))
    {
        (void)lfs_file_seek(&s_lfs, &file, (lfs_soff_t)(info->size - sizeof(rec)), LFS_SEEK_SET);
        if (lfs_file_read(&s_lfs, &file, &rec, sizeof(rec)) == (lfs_ssize_t)sizeof(rec))
        {
            info->last_epoch_sec = rec.epoch_sec;
        }
    }

    (void)lfs_file_close(&s_lfs, &file);
}

static uint16_t prv_collect_files(GW_StorageFileInfo_t* out, uint16_t max_items)
{
    lfs_dir_t dir;
    struct lfs_info info;
    uint16_t cnt = 0u;

    if (lfs_dir_open(&s_lfs, &dir, "/") != 0)
    {
        return 0u;
    }

    while (lfs_dir_read(&s_lfs, &dir, &info) > 0)
    {
        if ((info.type != LFS_TYPE_REG) || !prv_is_gwd_name(info.name))
        {
            continue;
        }
        if ((out != NULL) && (cnt < max_items))
        {
            memset(&out[cnt], 0, sizeof(out[cnt]));
            (void)snprintf(out[cnt].name, sizeof(out[cnt].name), "%s", info.name);
            out[cnt].size = info.size;
            out[cnt].rec_count = (uint16_t)(info.size / sizeof(GW_FileRec_t));
            prv_fill_file_bounds(&out[cnt]);
        }
        cnt++;
    }

    (void)lfs_dir_close(&s_lfs, &dir);

    if ((out != NULL))
    {
        uint16_t n = (cnt > max_items) ? max_items : cnt;
        if (n > 1u)
        {
            qsort(out, n, sizeof(out[0]), prv_cmp_file_desc);
        }
        for (uint16_t i = 0u; i < n; i++)
        {
            out[i].list_index = (uint16_t)(i + 1u);
        }
    }

    return cnt;
}

static bool prv_get_file_by_index(uint16_t idx1, GW_StorageFileInfo_t* out)
{
    GW_StorageFileInfo_t items[GW_STORAGE_MAX_FILES];
    uint16_t cnt = prv_collect_files(items, GW_STORAGE_MAX_FILES);

    if ((idx1 == 0u) || (idx1 > cnt) || (idx1 > GW_STORAGE_MAX_FILES))
    {
        return false;
    }
    if (out != NULL)
    {
        *out = items[idx1 - 1u];
    }
    return true;
}

static bool prv_stream_file(const GW_StorageFileInfo_t* info, uint32_t display_index, GW_StorageReadCb_t cb, void* user)
{
    char path[80];
    lfs_file_t file;
    uint32_t rec_idx = 0u;
    GW_FileRec_t rec;

    if ((info == NULL) || (cb == NULL))
    {
        return false;
    }

    (void)display_index;
    (void)snprintf(path, sizeof(path), "/%s", info->name);
    if (lfs_file_open(&s_lfs, &file, path, LFS_O_RDONLY) != 0)
    {
        return false;
    }

    while (lfs_file_read(&s_lfs, &file, &rec, sizeof(rec)) == (lfs_ssize_t)sizeof(rec))
    {
        uint16_t crc = UI_CRC16_CCITT((const uint8_t*)&rec,
                                      sizeof(rec) - sizeof(rec.crc16),
                                      UI_CRC16_INIT);
        if (crc != rec.crc16)
        {
            continue;
        }
        if (!cb(info, &rec, rec_idx, user))
        {
            (void)lfs_file_close(&s_lfs, &file);
            return false;
        }
        rec_idx++;
    }

    (void)lfs_file_close(&s_lfs, &file);
    return true;
}

static void prv_tcp_queue_init(GW_StorageTcpQueueFile_t* q)
{
    if (q == NULL)
    {
        return;
    }

    memset(q, 0, sizeof(*q));
    q->hdr.magic = GW_STORAGE_TCP_QUEUE_MAGIC;
    q->hdr.version = GW_STORAGE_TCP_QUEUE_VER;
}

static bool prv_tcp_queue_ref_equal(const GW_StorageRecRef_t* a, const GW_StorageRecRef_t* b)
{
    if ((a == NULL) || (b == NULL))
    {
        return false;
    }

    return ((a->day_epoch_sec == b->day_epoch_sec) &&
            (a->rec_index == b->rec_index));
}

static bool prv_tcp_queue_save(const GW_StorageTcpQueueFile_t* q)
{
    lfs_file_t file;
    int rc;

    if (q == NULL)
    {
        return false;
    }

    rc = lfs_file_open(&s_lfs,
                       &file,
                       GW_STORAGE_TCP_QUEUE_PATH,
                       LFS_O_WRONLY | LFS_O_CREAT | LFS_O_TRUNC);
    if (rc != 0)
    {
        return false;
    }

    rc = (lfs_file_write(&s_lfs, &file, q, sizeof(*q)) == (lfs_ssize_t)sizeof(*q)) ? 0 : -1;
    (void)lfs_file_sync(&s_lfs, &file);
    (void)lfs_file_close(&s_lfs, &file);
    return (rc == 0);
}

static bool prv_tcp_queue_load(GW_StorageTcpQueueFile_t* q, bool create_if_missing)
{
    lfs_file_t file;
    int rc;

    if (q == NULL)
    {
        return false;
    }

    prv_tcp_queue_init(q);

    rc = lfs_file_open(&s_lfs, &file, GW_STORAGE_TCP_QUEUE_PATH, LFS_O_RDONLY);
    if (rc != 0)
    {
        return create_if_missing ? prv_tcp_queue_save(q) : false;
    }

    rc = (lfs_file_read(&s_lfs, &file, q, sizeof(*q)) == (lfs_ssize_t)sizeof(*q)) ? 0 : -1;
    (void)lfs_file_close(&s_lfs, &file);
    if (rc != 0)
    {
        prv_tcp_queue_init(q);
        return create_if_missing ? prv_tcp_queue_save(q) : false;
    }

    if ((q->hdr.magic != GW_STORAGE_TCP_QUEUE_MAGIC) ||
        (q->hdr.version != GW_STORAGE_TCP_QUEUE_VER) ||
        (q->hdr.head >= GW_STORAGE_TCP_QUEUE_CAPACITY) ||
        (q->hdr.count > GW_STORAGE_TCP_QUEUE_CAPACITY))
    {
        prv_tcp_queue_init(q);
        return create_if_missing ? prv_tcp_queue_save(q) : false;
    }

    return true;
}

static bool prv_tcp_queue_remove_day_inplace(GW_StorageTcpQueueFile_t* q, uint32_t day_epoch_sec)
{
    GW_StorageRecRef_t keep[GW_STORAGE_TCP_QUEUE_CAPACITY];
    uint16_t kept = 0u;
    uint16_t i;

    if (q == NULL)
    {
        return false;
    }

    for (i = 0u; i < q->hdr.count; i++)
    {
        uint16_t idx = (uint16_t)((q->hdr.head + i) % GW_STORAGE_TCP_QUEUE_CAPACITY);
        if (q->items[idx].day_epoch_sec != day_epoch_sec)
        {
            keep[kept++] = q->items[idx];
        }
    }

    memset(q->items, 0, sizeof(q->items));
    q->hdr.head = 0u;
    q->hdr.count = kept;
    for (i = 0u; i < kept; i++)
    {
        q->items[i] = keep[i];
    }
    return true;
}

#endif

void GW_Storage_Init(void)
{
#if (UI_USE_LITTLEFS == 1)
    if (!prv_mount_fs(true))
    {
        return;
    }
    prv_unmount_fs();
#endif
}

bool GW_Storage_SaveHourRec(const GW_HourRec_t* rec)
{
#if (UI_USE_LITTLEFS == 1)
    const UI_Config_t* cfg;
    GW_FileRec_t file_rec;
    lfs_file_t file;
    char path[32];
    int rc = -1;

    if (rec == NULL)
    {
        return false;
    }
    if (!prv_mount_fs(true))
    {
        return false;
    }

    cfg = UI_GetConfig();
    memset(&file_rec, 0xFF, sizeof(file_rec));
    memcpy(file_rec.net_id, cfg->net_id, UI_NET_ID_LEN);
    file_rec.gw_num = cfg->gw_num;
    file_rec.rec_type = 1u;
    file_rec.epoch_sec = rec->epoch_sec;
    memcpy(&file_rec.rec, rec, sizeof(*rec));
    file_rec.crc16 = UI_CRC16_CCITT((const uint8_t*)&file_rec,
                                    sizeof(file_rec) - sizeof(file_rec.crc16),
                                    UI_CRC16_INIT);

    prv_make_day_path(rec->epoch_sec, path, sizeof(path));

    rc = lfs_file_open(&s_lfs, &file, path, LFS_O_WRONLY | LFS_O_CREAT | LFS_O_APPEND);
    if (rc == 0)
    {
        lfs_ssize_t wr = lfs_file_write(&s_lfs, &file, &file_rec, sizeof(file_rec));
        rc = (wr == (lfs_ssize_t)sizeof(file_rec)) ? 0 : -1;
        (void)lfs_file_sync(&s_lfs, &file);
        (void)lfs_file_close(&s_lfs, &file);
    }

    prv_unmount_fs();
    return (rc == 0);
#else
    (void)rec;
    return false;
#endif
}

void GW_Storage_PurgeOldFiles(uint32_t now_epoch_sec)
{
#if (UI_USE_LITTLEFS == 1)
    lfs_dir_t dir;
    struct lfs_info info;
    int32_t now_day_idx;

    if (!prv_mount_fs(false))
    {
        return;
    }

    now_day_idx = prv_day_index_from_epoch(now_epoch_sec);
    if (lfs_dir_open(&s_lfs, &dir, "/") == 0)
    {
        while (lfs_dir_read(&s_lfs, &dir, &info) > 0)
        {
            int32_t day_idx;
            if (info.type != LFS_TYPE_REG)
            {
                continue;
            }
            day_idx = prv_day_index_from_name(info.name);
            if (day_idx < 0)
            {
                continue;
            }
            if ((now_day_idx - day_idx) > 60)
            {
                char path[80];
                (void)snprintf(path, sizeof(path), "/%s", info.name);
                (void)lfs_remove(&s_lfs, path);
            }
        }
        (void)lfs_dir_close(&s_lfs, &dir);
    }

    prv_unmount_fs();
#else
    (void)now_epoch_sec;
#endif
}

uint16_t GW_Storage_ListFiles(GW_StorageFileInfo_t* out, uint16_t max_items)
{
#if (UI_USE_LITTLEFS == 1)
    uint16_t cnt;
    if (!prv_mount_fs(false))
    {
        return 0u;
    }
    cnt = prv_collect_files(out, max_items);
    prv_unmount_fs();
    return (cnt > max_items) ? max_items : cnt;
#else
    (void)out;
    (void)max_items;
    return 0u;
#endif
}

bool GW_Storage_ReadAllFiles(GW_StorageReadCb_t cb, void* user)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageFileInfo_t items[GW_STORAGE_MAX_FILES];
    uint16_t cnt;
    uint16_t i;

    if (cb == NULL)
    {
        return false;
    }
    if (!prv_mount_fs(false))
    {
        return false;
    }
    cnt = prv_collect_files(items, GW_STORAGE_MAX_FILES);
    for (i = 0u; i < cnt && i < GW_STORAGE_MAX_FILES; i++)
    {
        if (!prv_stream_file(&items[i], (uint32_t)(i + 1u), cb, user))
        {
            prv_unmount_fs();
            return false;
        }
    }
    prv_unmount_fs();
    return true;
#else
    (void)cb;
    (void)user;
    return false;
#endif
}

bool GW_Storage_ReadFileByIndex(uint16_t list_index_1based, GW_StorageReadCb_t cb, void* user)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageFileInfo_t item;
    bool ok;

    if ((cb == NULL) || !prv_mount_fs(false))
    {
        return false;
    }
    ok = prv_get_file_by_index(list_index_1based, &item) && prv_stream_file(&item, list_index_1based, cb, user);
    prv_unmount_fs();
    return ok;
#else
    (void)list_index_1based;
    (void)cb;
    (void)user;
    return false;
#endif
}

bool GW_Storage_DeleteAllFiles(void)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageFileInfo_t items[GW_STORAGE_MAX_FILES];
    uint16_t cnt;
    uint16_t i;

    if (!prv_mount_fs(false))
    {
        return false;
    }
    cnt = prv_collect_files(items, GW_STORAGE_MAX_FILES);
    for (i = 0u; i < cnt && i < GW_STORAGE_MAX_FILES; i++)
    {
        char path[80];
        (void)snprintf(path, sizeof(path), "/%s", items[i].name);
        (void)lfs_remove(&s_lfs, path);
    }
    {
        GW_StorageTcpQueueFile_t q;
        prv_tcp_queue_init(&q);
        (void)prv_tcp_queue_save(&q);
    }
    prv_unmount_fs();
    return true;
#else
    return false;
#endif
}

bool GW_Storage_DeleteFileByIndex(uint16_t list_index_1based)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageFileInfo_t item;
    char path[80];
    bool ok = false;
    int32_t day_index;

    if (!prv_mount_fs(false))
    {
        return false;
    }
    if (prv_get_file_by_index(list_index_1based, &item))
    {
        (void)snprintf(path, sizeof(path), "/%s", item.name);
        ok = (lfs_remove(&s_lfs, path) == 0);
        if (ok)
        {
            day_index = prv_day_index_from_name(item.name);
            if (day_index >= 0)
            {
                GW_StorageTcpQueueFile_t q;
                if (prv_tcp_queue_load(&q, true))
                {
                    (void)prv_tcp_queue_remove_day_inplace(&q, (uint32_t)day_index * 86400u);
                    (void)prv_tcp_queue_save(&q);
                }
            }
        }
    }
    prv_unmount_fs();
    return ok;
#else
    (void)list_index_1based;
    return false;
#endif
}


bool GW_Storage_FindRecordRefByEpoch(uint32_t epoch_sec, GW_StorageRecRef_t* out)
{
#if (UI_USE_LITTLEFS == 1)
    char path[32];
    lfs_file_t file;
    GW_FileRec_t rec;
    uint32_t rec_index = 0u;
    uint32_t day_epoch_sec = prv_day_epoch_from_epoch(epoch_sec);
    bool found = false;

    if (out == NULL)
    {
        return false;
    }

    if (!prv_mount_fs(false))
    {
        return false;
    }

    prv_make_day_path(day_epoch_sec, path, sizeof(path));
    if (lfs_file_open(&s_lfs, &file, path, LFS_O_RDONLY) == 0)
    {
        while (lfs_file_read(&s_lfs, &file, &rec, sizeof(rec)) == (lfs_ssize_t)sizeof(rec))
        {
            uint16_t crc = UI_CRC16_CCITT((const uint8_t*)&rec,
                                          sizeof(rec) - sizeof(rec.crc16),
                                          UI_CRC16_INIT);
            if ((crc == rec.crc16) && (rec.epoch_sec == epoch_sec))
            {
                out->day_epoch_sec = day_epoch_sec;
                out->rec_index = (uint16_t)rec_index;
                out->reserved = 0u;
                found = true;
                break;
            }
            rec_index++;
        }
        (void)lfs_file_close(&s_lfs, &file);
    }

    prv_unmount_fs();
    return found;
#else
    (void)epoch_sec;
    (void)out;
    return false;
#endif
}

bool GW_Storage_ReadHourRecByRef(const GW_StorageRecRef_t* ref, GW_HourRec_t* out)
{
#if (UI_USE_LITTLEFS == 1)
    char path[32];
    lfs_file_t file;
    GW_FileRec_t rec;
    bool ok = false;

    if ((ref == NULL) || (out == NULL))
    {
        return false;
    }

    if (!prv_mount_fs(false))
    {
        return false;
    }

    prv_make_day_path(ref->day_epoch_sec, path, sizeof(path));
    if (lfs_file_open(&s_lfs, &file, path, LFS_O_RDONLY) == 0)
    {
        lfs_soff_t off = (lfs_soff_t)ref->rec_index * (lfs_soff_t)sizeof(GW_FileRec_t);
        if ((lfs_file_seek(&s_lfs, &file, off, LFS_SEEK_SET) == off) &&
            (lfs_file_read(&s_lfs, &file, &rec, sizeof(rec)) == (lfs_ssize_t)sizeof(rec)))
        {
            uint16_t crc = UI_CRC16_CCITT((const uint8_t*)&rec,
                                          sizeof(rec) - sizeof(rec.crc16),
                                          UI_CRC16_INIT);
            if (crc == rec.crc16)
            {
                *out = rec.rec;
                ok = true;
            }
        }
        (void)lfs_file_close(&s_lfs, &file);
    }

    prv_unmount_fs();
    return ok;
#else
    (void)ref;
    (void)out;
    return false;
#endif
}

uint16_t GW_Storage_TcpQueue_Count(void)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageTcpQueueFile_t q;
    uint16_t count = 0u;

    if (!prv_mount_fs(false))
    {
        return 0u;
    }

    if (prv_tcp_queue_load(&q, true))
    {
        count = q.hdr.count;
    }

    prv_unmount_fs();
    return count;
#else
    return 0u;
#endif
}

uint32_t GW_Storage_TcpQueue_DropCount(void)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageTcpQueueFile_t q;
    uint32_t drop_count = 0u;

    if (!prv_mount_fs(false))
    {
        return 0u;
    }

    if (prv_tcp_queue_load(&q, true))
    {
        drop_count = q.hdr.drop_count;
    }

    prv_unmount_fs();
    return drop_count;
#else
    return 0u;
#endif
}

bool GW_Storage_TcpQueue_Push(const GW_StorageRecRef_t* ref, uint16_t max_keep)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageTcpQueueFile_t q;
    uint16_t cap;
    uint16_t i;
    uint16_t tail;
    bool ok;

    if (ref == NULL)
    {
        return false;
    }

    if (!prv_mount_fs(true))
    {
        return false;
    }

    if (!prv_tcp_queue_load(&q, true))
    {
        prv_unmount_fs();
        return false;
    }

    for (i = 0u; i < q.hdr.count; i++)
    {
        uint16_t idx = (uint16_t)((q.hdr.head + i) % GW_STORAGE_TCP_QUEUE_CAPACITY);
        if (prv_tcp_queue_ref_equal(&q.items[idx], ref))
        {
            prv_unmount_fs();
            return true;
        }
    }

    cap = max_keep;
    if ((cap == 0u) || (cap > GW_STORAGE_TCP_QUEUE_CAPACITY))
    {
        cap = GW_STORAGE_TCP_QUEUE_CAPACITY;
    }

    while (q.hdr.count >= cap)
    {
        q.hdr.head = (uint16_t)((q.hdr.head + 1u) % GW_STORAGE_TCP_QUEUE_CAPACITY);
        q.hdr.count--;
        q.hdr.drop_count++;
    }

    tail = (uint16_t)((q.hdr.head + q.hdr.count) % GW_STORAGE_TCP_QUEUE_CAPACITY);
    q.items[tail] = *ref;
    q.hdr.count++;

    ok = prv_tcp_queue_save(&q);
    prv_unmount_fs();
    return ok;
#else
    (void)ref;
    (void)max_keep;
    return false;
#endif
}

bool GW_Storage_TcpQueue_Peek(uint16_t order, GW_StorageRecRef_t* out)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageTcpQueueFile_t q;
    bool ok = false;

    if (out == NULL)
    {
        return false;
    }

    if (!prv_mount_fs(false))
    {
        return false;
    }

    if (prv_tcp_queue_load(&q, true) && (order < q.hdr.count))
    {
        uint16_t idx = (uint16_t)((q.hdr.head + order) % GW_STORAGE_TCP_QUEUE_CAPACITY);
        *out = q.items[idx];
        ok = true;
    }

    prv_unmount_fs();
    return ok;
#else
    (void)order;
    (void)out;
    return false;
#endif
}

bool GW_Storage_TcpQueue_PopN(uint16_t count)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageTcpQueueFile_t q;
    bool ok;

    if (!prv_mount_fs(true))
    {
        return false;
    }

    if (!prv_tcp_queue_load(&q, true))
    {
        prv_unmount_fs();
        return false;
    }

    if (count >= q.hdr.count)
    {
        q.hdr.count = 0u;
        q.hdr.head = 0u;
    }
    else
    {
        q.hdr.head = (uint16_t)((q.hdr.head + count) % GW_STORAGE_TCP_QUEUE_CAPACITY);
        q.hdr.count = (uint16_t)(q.hdr.count - count);
    }

    ok = prv_tcp_queue_save(&q);
    prv_unmount_fs();
    return ok;
#else
    (void)count;
    return false;
#endif
}

bool GW_Storage_TcpQueue_Clear(void)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageTcpQueueFile_t q;
    bool ok;

    if (!prv_mount_fs(true))
    {
        return false;
    }

    prv_tcp_queue_init(&q);
    ok = prv_tcp_queue_save(&q);
    prv_unmount_fs();
    return ok;
#else
    return false;
#endif
}


bool GW_Storage_TcpQueue_RemoveDay(uint32_t day_epoch_sec)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageTcpQueueFile_t q;
    bool ok;

    if (!prv_mount_fs(true))
    {
        return false;
    }
    if (!prv_tcp_queue_load(&q, true))
    {
        prv_unmount_fs();
        return false;
    }

    (void)prv_tcp_queue_remove_day_inplace(&q, day_epoch_sec);
    ok = prv_tcp_queue_save(&q);
    prv_unmount_fs();
    return ok;
#else
    (void)day_epoch_sec;
    return false;
#endif
}
