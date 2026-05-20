/*
 * quicbit — typed zero-copy messaging, C ABI.
 *
 * Handle-based opaque API. All call-status functions return 0 on
 * success or a negative error code; details are available from
 * `quicbit_last_error()` (thread-local, valid until the next call
 * from the same thread).
 *
 * The C ABI exposes a byte-oriented view of the local SHM
 * transport. Typed payload types are the caller's responsibility:
 * the FFI moves `slot_size`-byte blobs in and out of slots.
 */

#ifndef QUICBIT_H
#define QUICBIT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handles. */
typedef struct quicbit_service    quicbit_service_t;
typedef struct quicbit_publisher  quicbit_publisher_t;
typedef struct quicbit_subscriber quicbit_subscriber_t;
typedef struct quicbit_loan       quicbit_loan_t;
typedef struct quicbit_sample     quicbit_sample_t;

/* --- thread-local last error --- */
const char *quicbit_last_error(void);

/* --- service (local SHM) ---
 *
 * `name`         : NUL-terminated service name (POSIX SHM key).
 * `slot_count`   : number of slots in the pool.
 * `slot_size`    : per-slot payload capacity in bytes.
 * `history_depth`: number of past samples a late subscriber can see.
 *
 * `*_create` does `O_CREAT|O_EXCL`. Use `*_open_or_create` to attach
 * if the segment already exists with compatible parameters.
 */
quicbit_service_t *quicbit_service_create(
    const char *name,
    uint32_t    slot_count,
    uint32_t    slot_size,
    uint32_t    history_depth);

quicbit_service_t *quicbit_service_attach(const char *name);

quicbit_service_t *quicbit_service_open_or_create(
    const char *name,
    uint32_t    slot_count,
    uint32_t    slot_size,
    uint32_t    history_depth);

void quicbit_service_free(quicbit_service_t *svc);

/* --- publisher --- */
quicbit_publisher_t *quicbit_publisher_new(quicbit_service_t *svc);
void                 quicbit_publisher_free(quicbit_publisher_t *p);

/* Reserve a slot. On success, `*out_loan` is set; `*out_ptr` points
 * to writable payload bytes of size equal to the service's
 * `slot_size`. Returns 0 on success, negative on error. */
int quicbit_publisher_loan(
    quicbit_publisher_t *p,
    quicbit_loan_t     **out_loan,
    uint8_t            **out_ptr,
    size_t              *out_len);

/* Publish a loan. Consumes the loan handle; do not free it after. */
int quicbit_publisher_publish(
    quicbit_publisher_t *p,
    quicbit_loan_t      *loan);

/* Abort a loan without publishing; returns the slot to the pool. */
int quicbit_loan_abort(quicbit_loan_t *loan);

/* --- subscriber --- */
quicbit_subscriber_t *quicbit_subscriber_new(quicbit_service_t *svc);
void                  quicbit_subscriber_free(quicbit_subscriber_t *s);

/* Non-blocking take. Returns:
 *   0  — a sample is available; `*out_sample`, `*out_ptr`, `*out_len`
 *        are set (`out_ptr` is read-only; valid until sample_free).
 *   1  — no sample available (none of the out-params are set).
 *   <0 — error.
 */
int quicbit_subscriber_take(
    quicbit_subscriber_t *s,
    quicbit_sample_t    **out_sample,
    const uint8_t       **out_ptr,
    size_t               *out_len);

void quicbit_sample_free(quicbit_sample_t *sample);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* QUICBIT_H */
