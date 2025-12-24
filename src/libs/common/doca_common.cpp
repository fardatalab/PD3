#include "doca_common.hpp"


/*
 * Copyright (c) 2022 NVIDIA CORPORATION & AFFILIATES, ALL RIGHTS RESERVED.
 *
 * This software product is a proprietary product of NVIDIA CORPORATION &
 * AFFILIATES (the "Company") and all right, title, and interest in and to the
 * software product, including all associated intellectual property rights, are
 * and shall remain exclusively with the Company.
 *
 * This software product is governed by the End User License Agreement
 * provided with the software product.
 *
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_ctx.h>
#include <doca_dev.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_mmap.h>
#include <doca_pe.h>

DOCA_LOG_REGISTER(COMMON)

#define MAX_REP_PROPERTY_LEN 128

doca_error_t
open_doca_device_with_pci(const char *pci_addr, jobs_check func, struct doca_dev **retval)
{
	struct doca_devinfo **dev_list;
	uint32_t nb_devs;

	/* Set default return value */
	*retval = NULL;

	doca_error_t res = doca_devinfo_create_list(&dev_list, &nb_devs);
	if (res != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to open list of DOCA devices: %s", doca_error_get_name(res));
		return res;
	}

	/* Search */
	for (size_t i = 0; i < nb_devs; ++i) {
		uint8_t is_equal = 0;
		res = doca_devinfo_is_equal_pci_addr(dev_list[i], pci_addr, &is_equal);
		if (res == DOCA_SUCCESS && is_equal) {
			/* If any special capabilities are needed */
			if (func != nullptr && func(dev_list[i]) != DOCA_SUCCESS)
				continue;

			/* if device can be opened */
			res = doca_dev_open(dev_list[i], retval);
			if (res == DOCA_SUCCESS) {
				doca_devinfo_destroy_list(dev_list);
				return res;
			}
		}
	}

	DOCA_LOG_WARN("Matching device not found");
	res = doca_devinfo_destroy_list(dev_list);
	if (res != DOCA_SUCCESS){
		DOCA_LOG_ERR("Failed to destroy list of DOCA devices: %s", doca_error_get_name(res));
	}
	return DOCA_ERROR_NOT_FOUND;
}


doca_error_t
host_init_core_objects(AppState* state)
{
	doca_error_t res = doca_mmap_create(&state->src_mmap);
	if (res != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to create mmap: %s", doca_error_get_name(res));
		return res;
	}

	res = doca_mmap_add_dev(state->src_mmap, state->dev);
	if (res != DOCA_SUCCESS)
		DOCA_LOG_ERR("Unable to add device to mmap: %s", doca_error_get_name(res));

	return res;
}

doca_error_t
create_core_objects(AppState* state, uint32_t max_bufs)
{
	doca_error_t res;
	struct doca_pe *pe;

	res = doca_mmap_create(&state->src_mmap);
	if (res != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to create source mmap: %s", doca_error_get_name(res));
		return res;
	}
	res = doca_mmap_add_dev(state->src_mmap, state->dev);
	if (res != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to add device to source mmap: %s", doca_error_get_name(res));
		doca_mmap_destroy(state->src_mmap);
		state->src_mmap = NULL;
		return res;
	}

	res = doca_mmap_create(&state->dst_mmap);
	if (res != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to create destination mmap: %s", doca_error_get_name(res));
		return res;
	}
	res = doca_mmap_add_dev(state->dst_mmap, state->dev);
	if (res != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to add device to destination mmap: %s", doca_error_get_name(res));
		doca_mmap_destroy(state->dst_mmap);
		state->dst_mmap = NULL;
		return res;
	}

	res = doca_buf_inventory_create(max_bufs, &state->buf_inv);
	if (res != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to create buffer inventory: %s", doca_error_get_name(res));
		return res;
	}

	res = doca_buf_inventory_start(state->buf_inv);
	if (res != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to start buffer inventory: %s", doca_error_get_name(res));
		return res;
	}

	res = doca_pe_create(&pe);
	if (res != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to create work queue: %s", doca_error_get_name(res));
		return res;
	} else
		state->pe = pe;

	return res;
}

doca_error_t
start_context(AppState* state)
{
	doca_error_t res = doca_pe_connect_ctx(state->pe, state->ctx);
	if (res != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to connect progress engine with context: %s", doca_error_get_name(res));
		doca_pe_destroy(state->pe);
		state->pe = nullptr;
	}

	res = doca_ctx_start(state->ctx);
	if (res != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to start lib context: %s", doca_error_get_name(res));
		state->ctx = nullptr;
		return res;
	}

	return res;
}

doca_error_t
init_core_objects(AppState* state, uint32_t workq_depth, uint32_t max_bufs)
{
	doca_error_t res = create_core_objects(state, max_bufs);
	if (res != DOCA_SUCCESS)
		return res;
	res = start_context(state);
	return res;
}

doca_error_t
destroy_core_objects(AppState* state)
{
	doca_error_t tmp_result, result = DOCA_SUCCESS;

	if (state->pe != NULL) {
		tmp_result = doca_pe_destroy(state->pe);
		if (tmp_result != DOCA_SUCCESS) {
			DOCA_ERROR_PROPAGATE(result, tmp_result);
			DOCA_LOG_ERR("Failed to destroy work queue: %s", doca_error_get_name(tmp_result));
		}
		state->pe = nullptr;
	}

	if (state->buf_inv != nullptr) {
		tmp_result = doca_buf_inventory_destroy(state->buf_inv);
		if (tmp_result != DOCA_SUCCESS) {
			DOCA_ERROR_PROPAGATE(result, tmp_result);
			DOCA_LOG_ERR("Failed to destroy buf inventory: %s", doca_error_get_name(tmp_result));
		}
		state->buf_inv = nullptr;
	}

	if (state->src_mmap != nullptr) {
		tmp_result = doca_mmap_destroy(state->src_mmap);
		if (tmp_result != DOCA_SUCCESS) {
			DOCA_ERROR_PROPAGATE(result, tmp_result);
			DOCA_LOG_ERR("Failed to destroy source mmap: %s", doca_error_get_name(tmp_result));
		}
		state->src_mmap = nullptr;
	}

	if (state->dst_mmap != nullptr) {
		tmp_result = doca_mmap_destroy(state->dst_mmap);
		if (tmp_result != DOCA_SUCCESS) {
			DOCA_ERROR_PROPAGATE(result, tmp_result);
			DOCA_LOG_ERR("Failed to destroy destination mmap: %s", doca_error_get_name(tmp_result));
		}
		state->dst_mmap = nullptr;
	}

	if (state->ctx != nullptr) {
		tmp_result = doca_ctx_stop(state->ctx);
		if (tmp_result != DOCA_SUCCESS) {
			DOCA_ERROR_PROPAGATE(result, tmp_result);
			DOCA_LOG_ERR("Unable to stop context: %s", doca_error_get_name(tmp_result));
		}
	}

	if (state->dev != nullptr) {
		tmp_result = doca_dev_close(state->dev);
		if (tmp_result != DOCA_SUCCESS) {
			DOCA_ERROR_PROPAGATE(result, tmp_result);
			DOCA_LOG_ERR("Failed to close device: %s", doca_error_get_name(tmp_result));
		}
		state->dev = nullptr;
	}

	return result;
}

void
host_destroy_core_objects(AppState* state)
{
	doca_error_t res;

	res = doca_mmap_destroy(state->src_mmap);
	if (res != DOCA_SUCCESS)
		DOCA_LOG_ERR("Failed to destroy mmap: %s", doca_error_get_name(res));
	state->src_mmap = NULL;

	res = doca_dev_close(state->dev);
	if (res != DOCA_SUCCESS)
		DOCA_LOG_ERR("Failed to close device: %s", doca_error_get_name(res));
	state->dev = NULL;
}

char *
hex_dump(const void *data, size_t size)
{
	/*
	 * <offset>:     <Hex bytes: 1-8>        <Hex bytes: 9-16>         <Ascii>
	 * 00000000: 31 32 33 34 35 36 37 38  39 30 61 62 63 64 65 66  1234567890abcdef
	 *    8     2         8 * 3          1          8 * 3         1       16       1
	 */
	const size_t line_size = 8 + 2 + 8 * 3 + 1 + 8 * 3 + 1 + 16 + 1;
	int i, j, r, read_index;
	size_t num_lines, buffer_size;
	char *buffer, *write_head;
	unsigned char cur_char, printable;
	char ascii_line[17];
	const unsigned char *input_buffer;

	/* Allocate a dynamic buffer to hold the full result */
	num_lines = (size + 16 - 1) / 16;
	buffer_size = num_lines * line_size + 1;
	buffer = (char *)malloc(buffer_size);
	if (buffer == NULL)
		return NULL;
	write_head = buffer;
	input_buffer = reinterpret_cast<const unsigned char*>(data);
	read_index = 0;

	for (i = 0; i < num_lines; i++)	{
		/* Offset */
		snprintf(write_head, buffer_size, "%08X: ", i * 16);
		write_head += 8 + 2;
		buffer_size -= 8 + 2;
		/* Hex print - 2 chunks of 8 bytes */
		for (r = 0; r < 2 ; r++) {
			for (j = 0; j < 8; j++) {
				/* If there is content to print */
				if (read_index < size) {
					cur_char = input_buffer[read_index++];
					snprintf(write_head, buffer_size, "%02X ", cur_char);
					/* Printable chars go "as-is" */
					if (' ' <= cur_char && cur_char <= '~')
						printable = cur_char;
					/* Otherwise, use a '.' */
					else
						printable = '.';
				/* Else, just use spaces */
				} else {
					snprintf(write_head, buffer_size, "   ");
					printable = ' ';
				}
				ascii_line[r * 8 + j] = printable;
				write_head += 3;
				buffer_size -= 3;
			}
			/* Spacer between the 2 hex groups */
			snprintf(write_head, buffer_size, " ");
			write_head += 1;
			buffer_size -= 1;
		}
		/* Ascii print */
		ascii_line[16] = '\0';
		snprintf(write_head, buffer_size, "%s\n", ascii_line);
		write_head += 16 + 1;
		buffer_size -= 16 + 1;
	}
	/* No need for the last '\n' */
	write_head[-1] = '\0';
	return buffer;
}
