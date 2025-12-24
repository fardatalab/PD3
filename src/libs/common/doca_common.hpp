#pragma once

#include <doca_error.h>
#include <doca_dev.h>

struct AppState {
  struct doca_dev* dev = nullptr;
  struct doca_mmap* src_mmap = nullptr;
  struct doca_mmap* dst_mmap = nullptr;
  struct doca_buf_inventory* buf_inv = nullptr;
  struct doca_ctx* ctx = nullptr;
  struct doca_pe* pe = nullptr;

  AppState() = default;
  ~AppState() = default;
};

/* Function to check if a given device is capable of executing some job */
using jobs_check = doca_error_t (*)(struct doca_devinfo *);

/*
 * Open a DOCA device according to a given PCI address
 *
 * @value [in]: PCI address
 * @func [in]: pointer to a function that checks if the device have some job capabilities (Ignored if set to NULL)
 * @retval [out]: pointer to doca_dev struct, NULL if not found
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t open_doca_device_with_pci(const char* pcie_addr, 
                                       jobs_check func,
					                             struct doca_dev **retval);


/*
 * Initialize the DOCA objects needed on the host 
 */
doca_error_t host_init_core_objects(AppState* state);

/*
 * Initialize a series of DOCA Core objects needed for the program's execution
 *
 * @state [in]: struct containing the set of initialized DOCA Core objects
 * @extensions [in]: bitmap of extensions enabled for the inventory described in doca_buf.h.
 * @workq_depth [in]: depth for the created Work Queue
 * @max_chunks [in]: maximum number of chunks for DOCA Mmap
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t init_core_objects(AppState* state, uint32_t workq_depth, uint32_t max_chunks);

/*
 * Cleanup the series of DOCA Core objects created by init_core_objects
 *
 * @state [in]: struct containing the set of initialized DOCA Core objects
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t destroy_core_objects(AppState* state);

/*
 * Destroys all DOCA core structures
 *
 * @state [in]: Structure containing all DOCA core structures
 */
void host_destroy_core_objects(AppState* state);

/*
 * Create a string Hex dump representation of the given input buffer
 *
 * @data [in]: Pointer to the input buffer
 * @size [in]: Number of bytes to be analyzed
 * @return: pointer to the string representation, or NULL if an error was encountered
 */
char *hex_dump(const void *data, size_t size);


