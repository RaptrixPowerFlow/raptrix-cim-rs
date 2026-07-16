#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

#define RPF_HEALTH_HEALTHY      0
#define RPF_HEALTH_CAUTION      1
#define RPF_HEALTH_STRESSED     2
#define RPF_HEALTH_PATHOLOGICAL 3

typedef struct RpfCaseHealthFFI {
    int32_t grade;
    double reactive_support_headroom_mvar;
    int32_t q_violation_count;
    int32_t solver_q_limit_infeasible_count;
    uint32_t island_count;
    uint32_t buses;
    double total_load_p_mw;
    double total_gen_p_mw;
    double base_mva;
    uint8_t detached_islands_present;
    uint8_t solve_data_present;
    uint32_t buses_out_of_voltage_band;
    int32_t pv_to_pq_switch_count;
} RpfCaseHealthFFI;

/* Reads an .rpf file and returns a heap-allocated health snapshot, or NULL on error.
 * path must be a valid, NUL-terminated UTF-8 string (including on Windows).
 * Free the result with free_rpf_case_health_ffi(). */
RpfCaseHealthFFI* inspect_rpf_file_c(const char* path);
void free_rpf_case_health_ffi(RpfCaseHealthFFI* ptr);

/* Returns a single-line flat JSON object for logging/debug, or NULL on error.
 * path must be a valid, NUL-terminated UTF-8 string (including on Windows).
 * Free the result with free_rpf_case_health_json(). */
char* inspect_rpf_file_c_json(const char* path);
void free_rpf_case_health_json(char* json);

/* Last error on the calling thread; valid until the next FFI call on that thread. */
const char* rpf_case_health_last_error(void);

/* Merge solver-owned tables from patch_rpf into source_rpf, writing output_rpf.
 * Converter-owned tables (GIS, contingencies, RAS, diagrams, unknown tables, …)
 * always come from source_rpf. Paths must be valid NUL-terminated UTF-8.
 * Returns 0 on success, -1 on error (see rpf_case_health_last_error). */
int apply_rpf_patch_c(const char* source_rpf, const char* patch_rpf, const char* output_rpf);

#ifdef __cplusplus
}
#endif
