// clang-format off
#include "c.h"
#include <postgres.h>
#include <fmgr.h>
// clang-format on
#include <executor/spi.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <utils/snapmgr.h>
#if PG_MAJORVERSION_NUM < 15
#include <access/xact.h>
#endif
#include <common/pg_prng.h>

PG_MODULE_MAGIC;

static int32 retry_attempts = 0;
static float8 cap_sleep_secs = 0.0001;    // 100 microseconds
static float8 base_sleep_secs = 0.000001; // microsecond

/**
 * The backoff should increase with each attempt.
 */
static float8 get_backoff(float8 cap, float8 base, int32 attempt) {
  float8 exp = (float8)(1 << attempt);
  return Min(cap, base * exp);
}

/**
 * Get the random jitter to avoid contention in the backoff. Uses the
 * process seed initialized in `InitProcessGlobals`.
 */
static float8 get_jitter() { return pg_prng_double(&pg_global_prng_state); }

/**
 * Implements the backoff + fitter approach
 * https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
 */
static float8 backoff_jitter(float8 cap, float8 base, int32 attempt) {
  return get_jitter() * get_backoff(cap, base, attempt);
}

PG_FUNCTION_INFO_V1(retry);

Datum retry(PG_FUNCTION_ARGS) {
  if (PG_ARGISNULL(0)) {
    ereport(ERROR, errmsg("transaction statements argument is required"));
  }
  if (IsTransactionBlock()) {
    ereport(ERROR, errmsg("can't be used inside of a transaction block"));
  }
  int max_attempts = 10;
  if (!PG_ARGISNULL(1)) {
    max_attempts = PG_GETARG_INT32(1);
  }

  text *stmts = PG_GETARG_TEXT_PP(0);
  char *cstmts = text_to_cstring(stmts);
  bool retry = true;
  retry_attempts = 0;
  while (retry) {
    XactIsoLevel = XACT_SERIALIZABLE;
    SPI_connect_ext(SPI_OPT_NONATOMIC);
    MemoryContext current_mcxt = CurrentMemoryContext;
    PG_TRY();
    {
      SPI_exec(cstmts, 0);
      SPI_finish();
      retry = false;
    }
    PG_CATCH();
    {
      MemoryContextSwitchTo(current_mcxt);
      SPI_finish();
      ErrorData *err = CopyErrorData();
      if (err->sqlerrcode == ERRCODE_T_R_SERIALIZATION_FAILURE) {
        ereport(NOTICE, errmsg("%d", max_attempts));
        PopActiveSnapshot();
        AbortCurrentTransaction();

        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());

        if (++retry_attempts > max_attempts) {
          ereport(ERROR, errcode(err->sqlerrcode),
                  errmsg("maximum number of retries (%d) has been attempted", max_attempts));
        } else {
          DirectFunctionCall1(pg_sleep,
                              Float8GetDatum(backoff_jitter(cap_sleep_secs, base_sleep_secs, retry_attempts)));
        }
      } else {
        retry_attempts = 0;
        PG_RE_THROW();
      }
      CHECK_FOR_INTERRUPTS();
      FlushErrorState();
    }

    PG_END_TRY();
  }

  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(current_retry_attempt);

Datum current_retry_attempt(PG_FUNCTION_ARGS) { PG_RETURN_INT32(retry_attempts); }
