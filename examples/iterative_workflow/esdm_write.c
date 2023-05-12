#include <esdm.h>
#include <esdm-datatypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#define HEIGHT 32
#define WIDTH  1
#define COUNT  150

int main(int argc, char const *argv[]) {

  float *buf_w = malloc(HEIGHT * WIDTH * sizeof(float)), *p = buf_w;

  for (int y = 0; y < HEIGHT; y++)
    for (int x = 0; x < WIDTH; x++, p++)
      *p = (float)(y * WIDTH + x);

  esdm_status ret;
  esdm_container_t *container = NULL;
  esdm_dataset_t *dataset = NULL;

  ret = esdm_init();
  assert(ret == ESDM_SUCCESS);

  esdm_simple_dspace_t dataspace = esdm_dataspace_2d(HEIGHT, WIDTH*COUNT, SMD_DTYPE_FLOAT);
  assert(dataspace.ptr);

  ret = esdm_container_create("etas.nc", 1, &container);
  assert(ret == ESDM_SUCCESS);

  ret = esdm_dataset_create(container, "tas", dataspace.ptr, &dataset);
  assert(ret == ESDM_SUCCESS);

  char *names[] = {"cell", "time"};
  ret = esdm_dataset_name_dims(dataset, names);
  assert(ret == ESDM_SUCCESS);

  float a = 1.e+20f;
  smd_attr_t *attr1 = smd_attr_new("_FillValue", SMD_DTYPE_FLOAT, &a);
  ret = esdm_dataset_link_attribute(dataset, 0, attr1);
  assert(ret == ESDM_SUCCESS);
  ret = esdm_dataset_set_fill_value(dataset, &a);
  assert(ret == ESDM_SUCCESS);

  ret = esdm_dataset_commit(dataset);
  assert(ret == ESDM_SUCCESS);
  ret = esdm_container_commit(container);
  assert(ret == ESDM_SUCCESS);

  for (int n = 0 ; n < COUNT; n++) {

    esdm_simple_dspace_t subspace = esdm_dataspace_2do(0, HEIGHT, n*WIDTH, WIDTH, SMD_DTYPE_FLOAT);
    assert(subspace.ptr);

    ret = esdm_write(dataset, buf_w, subspace.ptr);
    assert(ret == ESDM_SUCCESS);

    ret = esdm_dataset_commit(dataset);
    assert(ret == ESDM_SUCCESS);

    printf("Step %d written\n", n);

    sleep(2);
  }

  ret = esdm_dataset_close(dataset);
  assert(ret == ESDM_SUCCESS);
  ret = esdm_container_close(container);
  assert(ret == ESDM_SUCCESS);

  ret = esdm_finalize();
  assert(ret == ESDM_SUCCESS);

  free(buf_w);

  return 0;
}

