#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <atomic>
#include <unistd.h>
#include <emmintrin.h>
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace buffermanager {
// -------------------------------------------------------------------------------------
struct RestartException {
public:
   RestartException() {}
   RestartException(int code)
   {
      cout << code << endl;
   }
};
// -------------------------------------------------------------------------------------
class ReadGuard;
class ExclusiveGuard;
template<typename T>
class ReadPageGuard;
using lock_version_t = u64;
using OptimisticLock = atomic<lock_version_t>;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
class ReadGuard {
   friend class ExclusiveGuard;
   template<typename T>
   friend
   class ReadPageGuard;
   template<typename T>
   friend
   class WritePageGuard;
private:
   ReadGuard(atomic<u64> *version_ptr, u64 local_version)
           :
           version_ptr(version_ptr)
           , local_version(local_version) {}

public:
   atomic<u64> *version_ptr = nullptr;
   u64 local_version;
   // -------------------------------------------------------------------------------------
   ReadGuard() = default;
   // -------------------------------------------------------------------------------------
   ReadGuard(OptimisticLock &lock)
           : version_ptr(&lock)
   {
      local_version = version_ptr->load();
      if ((local_version & 2) == 2 ) {
         spin();
      }
      assert((local_version & 2) != 2);
   }
   // -------------------------------------------------------------------------------------
   inline void recheck()
   {
      if ( local_version != *version_ptr ) {
         throw RestartException();
      }
   }
   // -------------------------------------------------------------------------------------
   void spin();
   // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
class ExclusiveGuard {
private:
   ReadGuard &ref_guard; // our basis
public:
   // -------------------------------------------------------------------------------------
   ExclusiveGuard(ReadGuard &read_lock);
   // -------------------------------------------------------------------------------------
   ~ExclusiveGuard();
};
// -------------------------------------------------------------------------------------
// TODO: Shared guard for scans
/*
 * Plan:
 * SharedGuard control the LSB 6-bits
 * Exclusive bit is the LSB 7th bit
 * TODO: rewrite the read and exclusive guards
 */
// The constants
constexpr u64 exclusive_bit = 1 << 7;
constexpr u64 shared_bit = 1 << 0;
class SharedGuard {
private:
   ReadGuard &ref_guard;
public:
   SharedGuard(ReadGuard &read_guard);
};
// -------------------------------------------------------------------------------------
}
}