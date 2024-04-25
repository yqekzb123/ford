// author: huangchunyue
#include "glm.h"
#include <iostream>
#include <thread>
#include <vector>
#include <mutex>

int main(){
    GLM* glm = new GLM();
    // 计时
    struct timespec tx_start_time;
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    // 生成32线程，每个线程各自请求锁
    std::vector<std::thread> threads;
    for(int i=0; i<32; i++){
        threads.push_back(std::thread([i, glm](){
            int cnt = 0;
            while(cnt < 100000){ // 模拟业务处理(1s
                std::vector<glmlock_request> requests;
                glmlock_request req;
                req.table_id = 1;
                req.page_id = i*100000+cnt;
                requests.push_back(req);
                int pos1 = glm->GLMLockShared(requests);
                if(pos1 != -1){
                    std::cout << "Thread " << i << " shared lock failed" << std::endl;
                }
                // int pos2 = glm->GLMLockExclusive(requests);
                // if(pos2 != -1){
                //     std::cout << "Thread " << i << " exclusive lock failed" << std::endl;
                // }
                glm->GLMUnLockShared(requests);
                // glm->GLMUnLockExclusive(requests);
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
}