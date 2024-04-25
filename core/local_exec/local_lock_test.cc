#include "local_lock.h"

int main(){
    LocalLockStore* local_lock_store = new LocalLockStore();
    // 计时
    struct timespec tx_start_time;
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    // 生成32线程，每个线程各自请求锁
    std::vector<std::thread> threads;
    for(int i=0; i<32; i++){
        threads.push_back(std::thread([i, local_lock_store](){
            int cnt = 0;
            while(cnt < 100000){ 
                int table_id = 1;
                itemkey_t key = i*100000+cnt;
                
                LocalLock* local_lock = local_lock_store->GetLock(table_id, key);
                bool success = local_lock->LockShared();
                if(!success){
                    std::cout << "Thread " << i << " shared lock failed" << std::endl;
                }

                local_lock = local_lock_store->GetLock(table_id, key);
                // success = local_lock->LockExclusive();
                if(!success){
                    std::cout << "Thread " << i << " exclusive lock failed" << std::endl;
                }
                local_lock->UnlockShared();
                // local_lock->UnlockExclusive();
                cnt++;
            }
        }));
    }
    for(auto& t : threads){
        t.join();
    }
    // 计算吞吐量
    struct timespec tx_end_time;
    clock_gettime(CLOCK_REALTIME, &tx_end_time);
    double tx_sec = (tx_end_time.tv_sec - tx_start_time.tv_sec);
    printf("Total time: %lf\n", tx_sec);
    printf("Throughput: %lf\n", 32*100000/tx_sec);   
    return 0;
}