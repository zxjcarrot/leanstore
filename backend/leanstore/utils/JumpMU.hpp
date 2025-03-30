#pragma once
#include <setjmp.h>
#include <signal.h>
#include <cstring>
#include <stdexcept>
#include <iostream>

#include <cassert>
#include <utility>

#define JUMPMU_STACK_SIZE 100
namespace jumpmu
{
extern __thread int checkpoint_counter;
extern __thread jmp_buf env[JUMPMU_STACK_SIZE];
extern __thread int checkpoint_stacks_counter[JUMPMU_STACK_SIZE];
extern __thread void (*de_stack_arr[JUMPMU_STACK_SIZE])(void*);
extern __thread void* de_stack_obj[JUMPMU_STACK_SIZE];
extern __thread int de_stack_counter;
extern __thread bool in_jump;
void jump();
inline void clearLastDestructor()
{
   de_stack_obj[de_stack_counter - 1] = nullptr;
   de_stack_arr[de_stack_counter - 1] = nullptr;
   de_stack_counter--;
   assert(de_stack_counter >= 0);
}

static void saveThreadLocalState(char* buf, size_t buf_size) {
  size_t offset = 0;

  // Save checkpoint_counter
  if (offset + sizeof(jumpmu::checkpoint_counter) > buf_size) {
      throw std::runtime_error("Buffer too small to save checkpoint_counter");
  }
  memcpy(buf + offset, &jumpmu::checkpoint_counter, sizeof(jumpmu::checkpoint_counter));
  offset += sizeof(jumpmu::checkpoint_counter);

  // Save env
  if (offset + sizeof(jumpmu::env) > buf_size) {
      throw std::runtime_error("Buffer too small to save env");
  }
  memcpy(buf + offset, &jumpmu::env, sizeof(jumpmu::env));
  offset += sizeof(jumpmu::env);


  // Save checkpoint_stacks_counter
  if (offset + sizeof(jumpmu::checkpoint_stacks_counter) > buf_size) {
      throw std::runtime_error("Buffer too small to save checkpoint_stacks_counter");
  }
  memcpy(buf + offset, &jumpmu::checkpoint_stacks_counter, sizeof(jumpmu::checkpoint_stacks_counter));
  offset += sizeof(jumpmu::checkpoint_stacks_counter);

  // Save de_stack_arr
  if (offset + sizeof(jumpmu::de_stack_arr) > buf_size) {
      throw std::runtime_error("Buffer too small to save de_stack_arr");
  }
  memcpy(buf + offset, &jumpmu::de_stack_arr, sizeof(jumpmu::de_stack_arr));
  offset += sizeof(jumpmu::de_stack_arr);

  // Save de_stack_obj
  if (offset + sizeof(jumpmu::de_stack_obj) > buf_size) {
      throw std::runtime_error("Buffer too small to save de_stack_obj");
  }
  memcpy(buf + offset, &jumpmu::de_stack_obj, sizeof(jumpmu::de_stack_obj));
  offset += sizeof(jumpmu::de_stack_obj);

  // Save de_stack_counter
  if (offset + sizeof(jumpmu::de_stack_counter) > buf_size) {
      throw std::runtime_error("Buffer too small to save de_stack_counter");
  }
  memcpy(buf + offset, &jumpmu::de_stack_counter, sizeof(jumpmu::de_stack_counter));
  offset += sizeof(jumpmu::de_stack_counter);

  // Save in_jump
  if (offset + sizeof(jumpmu::in_jump) > buf_size) {
      throw std::runtime_error("Buffer too small to save in_jump");
  }
  memcpy(buf + offset, &jumpmu::in_jump, sizeof(jumpmu::in_jump));
  offset += sizeof(jumpmu::in_jump);

  checkpoint_counter = 0;
  de_stack_counter = 0;
  in_jump = false;
}

static void restoreThreadLocalstate(char* buf, size_t buf_size) {
  size_t offset = 0;

  // Restore checkpoint_counter
  if (offset + sizeof(jumpmu::checkpoint_counter) > buf_size) {
      throw std::runtime_error("Buffer too small to restore checkpoint_counter");
  }
  memcpy(&jumpmu::checkpoint_counter, buf + offset, sizeof(jumpmu::checkpoint_counter));
  offset += sizeof(jumpmu::checkpoint_counter);

  // Restore env
  if (offset + sizeof(jumpmu::env) > buf_size) {
      throw std::runtime_error("Buffer too small to restore env");
  }
  memcpy(&jumpmu::env, buf + offset, sizeof(jumpmu::env));
  offset += sizeof(jumpmu::env);

  // Restore checkpoint_stacks_counter
  if (offset + sizeof(jumpmu::checkpoint_stacks_counter) > buf_size) {
      throw std::runtime_error("Buffer too small to restore checkpoint_stacks_counter");
  }
  memcpy(&jumpmu::checkpoint_stacks_counter, buf + offset, sizeof(jumpmu::checkpoint_stacks_counter));
  offset += sizeof(jumpmu::checkpoint_stacks_counter);

  // Restore de_stack_arr
  if (offset + sizeof(jumpmu::de_stack_arr) > buf_size) {
      throw std::runtime_error("Buffer too small to restore de_stack_arr");
  }
  memcpy(&jumpmu::de_stack_arr, buf + offset, sizeof(jumpmu::de_stack_arr));
  offset += sizeof(jumpmu::de_stack_arr);

  // Restore de_stack_obj
  if (offset + sizeof(jumpmu::de_stack_obj) > buf_size) {
      throw std::runtime_error("Buffer too small to restore de_stack_obj");
  }
  memcpy(&jumpmu::de_stack_obj, buf + offset, sizeof(jumpmu::de_stack_obj));
  offset += sizeof(jumpmu::de_stack_obj);

  // Restore de_stack_counter
  if (offset + sizeof(jumpmu::de_stack_counter) > buf_size) {
      throw std::runtime_error("Buffer too small to restore de_stack_counter");
  }
  memcpy(&jumpmu::de_stack_counter, buf + offset, sizeof(jumpmu::de_stack_counter));
  offset += sizeof(jumpmu::de_stack_counter);

  // Restore in_jump
  if (offset + sizeof(jumpmu::in_jump) > buf_size) {
      throw std::runtime_error("Buffer too small to restore in_jump");
  }
  memcpy(&jumpmu::in_jump, buf + offset, sizeof(jumpmu::in_jump));
  offset += sizeof(jumpmu::in_jump);
}
}  // namespace jumpmu
   // -------------------------------------------------------------------------------------
   // clang-format off
#define jumpmu_registerDestructor()                       \
  assert(jumpmu::de_stack_counter < JUMPMU_STACK_SIZE);assert(jumpmu::checkpoint_counter < JUMPMU_STACK_SIZE);jumpmu::de_stack_arr[jumpmu::de_stack_counter] = &des;assert(jumpmu::de_stack_arr[jumpmu::de_stack_counter]!=nullptr);jumpmu::de_stack_obj[jumpmu::de_stack_counter] = this;jumpmu::de_stack_counter++;

#define jumpmu_defineCustomDestructor(NAME) static void des(void* t) { reinterpret_cast<NAME*>(t)->~NAME(); }

// without calling destructors
#define jumpmu_return           \
  jumpmu::checkpoint_counter--; return

#define jumpmu_break                            \
  jumpmu::checkpoint_counter--; break

#define jumpmu_continue                         \
  jumpmu::checkpoint_counter--; continue

// ATTENTION DO NOT DO ANYTHING BETWEEN setjmp and if !!
#define jumpmuTry()                                                                         \
  assert(jumpmu::de_stack_counter >= 0); jumpmu::checkpoint_stacks_counter[jumpmu::checkpoint_counter] = jumpmu::de_stack_counter; int _lval = setjmp(jumpmu::env[jumpmu::checkpoint_counter++]); if (_lval == 0) {

#define jumpmuCatch()           \
  jumpmu::checkpoint_counter--; } else
   // clang-format on

template <typename T>
class JMUW
{
  public:
   T obj;
   template <typename... Args>
   JMUW(Args&&... args) : obj(std::forward<Args>(args)...)
   {
      jumpmu_registerDestructor();
   }
   static void des(void* t) { reinterpret_cast<JMUW<T>*>(t)->~JMUW<T>(); }
   ~JMUW() { jumpmu::clearLastDestructor(); }
   T* operator->() { return reinterpret_cast<T*>(&obj); }
};
