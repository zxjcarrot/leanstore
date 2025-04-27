#include "FreeList.hpp"

#include "Exceptions.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
void FreeList::batchPush(BufferFrame* batch_head, BufferFrame* batch_tail, u64 batch_counter)
{
   JMUW<std::unique_lock<std::mutex>> guard(mutex);
   batch_tail->header.next_free_bf = head;
   head = batch_head;
   counter += batch_counter;
}
// -------------------------------------------------------------------------------------
void FreeList::push(BufferFrame& bf)
{
   paranoid(bf.header.state == BufferFrame::STATE::FREE);
   bf.header.latch.assertNotExclusivelyLatched();
   // -------------------------------------------------------------------------------------
   JMUW<std::unique_lock<std::mutex>> guard(mutex);
   bf.header.next_free_bf = head;
   head = &bf;
   counter++;
}
// -------------------------------------------------------------------------------------
struct BufferFrame& FreeList::tryPop()
{
   //JMUW<std::unique_lock<std::mutex>> guard(mutex);
   if (!mutex.try_lock()) {
      jumpmu::jump();
   }
   BufferFrame* free_bf = head;
   if (head == nullptr) {
      mutex.unlock();
      jumpmu::jump();
   } else {
      head = head->header.next_free_bf;
      counter--;
      paranoid(free_bf->header.state == BufferFrame::STATE::FREE);
   }
   mutex.unlock();
   return *free_bf;
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
