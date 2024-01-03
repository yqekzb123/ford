import json
import os,sys,datetime,re
import subprocess

user = "zhy"
compute_node_ips = ['127.0.0.1']
page_table_node_ips = ['127.0.0.1']
data_node_ips = ['127.0.0.1']

common_config_path = "config/"
compute_node_config = "compute_node_config.json"
compute_node_path = "build/compute_pool/run"
compute_node = "run"

data_node_config = "memory_data_node_config.json"
data_node_path = "build/memory_pool/data_server"
data_node = "data_server"

page_table_node_config = "memory_page_table_node_config.json"
page_table_node_path = "build/memory_pool/page_table_server"
page_table_node = "page_table_server"

def start_remote_node(ip, config_path, config, exe_path, exe, node_type, cnt):
    # mkdir 
    cmd = "ssh {}@{} 'mkdir -p ~/WriteFlex/{} ~/WriteFlex/{};'".format(user,ip,config_path,exe_path)
    print cmd
    os.system(cmd)
    # copy file
    cmd = 'scp -r {}/{} {}@{}:~/WriteFlex/{}'.format(config_path, config, user, ip, config_path)
    print cmd
    os.system(cmd)
    cmd = 'scp -r {}/{} {}@{}:~/WriteFlex/{}'.format(exe_path, exe, user, ip, exe_path)
    print cmd
    os.system(cmd)

    if node_type == "compute_node":
        cmd = "ssh {}@{} 'cd ~/WriteFlex/{}; ./{} smallbank our 16 8 4 > ~/WriteFlex/{}_{} 2>&1'".format(user, ip, exe_path, exe, node_type, cnt)
    else:
        cmd = "ssh {}@{} 'cd ~/WriteFlex/{}; ./{} > ~/WriteFlex/{}_{} 2>&1'".format(user, ip, exe_path, exe, node_type, cnt)
    print cmd
    os.system(cmd)
    
if __name__ == "__main__":
    # i = 0
    # for ip, config_path, config, exe_path, exe in zip(compute_node_ips, [common_config_path] * len(compute_node_ips), [compute_node_config] * len(compute_node_ips), [compute_node_path] * len(compute_node_ips), [compute_node] * len(compute_node_ips)):
    #     start_remote_node(ip, config_path, config, exe_path, exe, "compute_node", i)
    #     i = i + 1

    i = 0
    for ip, config_path, config, exe_path, exe in zip(data_node_ips, [common_config_path] * len(data_node_ips), [data_node_config] * len(data_node_ips), [data_node_path] * len(data_node_ips), [data_node] * len(data_node_ips)):
        start_remote_node(ip, config_path, config, exe_path, exe, "data_node", i)
        i = i + 1
    
    i = 0
    for ip, config_path, config, exe_path, exe in zip(page_table_node_ips, [common_config_path] * len(page_table_node_ips), [page_table_node_config] * len(page_table_node_ips), [page_table_node_path] * len(page_table_node_ips), [page_table_node] * len(page_table_node_ips)):
        start_remote_node(ip, config_path, config, exe_path, exe, "page_table_node", i)
        i = i + 1
