import os
import time
import zipfile
import shutil
import concurrent.futures
from obs import ObsClient
from pathlib import Path

def join_path(*args):
    rlt = os.path.join(*args)
    return rlt.replace(os.sep, '/')

def get_current_time():
    return "["+str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))+"]"



class OBSClient():
    def __init__(self, ak, sk, endpoint, input_bucket_name, input_obs_path, output_bucket_name, output_obs_path):
        self.obs_client = ObsClient(access_key_id=ak, secret_access_key=sk, server=endpoint)
        self.input_bucket_name = input_bucket_name
        self.input_obs_path = input_obs_path
        self.output_bucket_name = output_bucket_name
        self.output_obs_path = output_obs_path
    
    def list_object(self, bucket_name, obs_path):
        
        if not str(obs_path).endswith("/"):
            obs_path += "/"
        resp = self.obs_client.listObjects(bucketName=bucket_name, prefix=obs_path, delimiter='/')
        return [x.key for x in resp.body.contents]
        
        
    def list_sub_dir(self, bucket_name, obs_path):
        
        result = []
        max_num = 1000
        mark = None

        while True:

            if not str(obs_path).endswith("/"):
                obs_path += "/"
            resp = self.obs_client.listObjects(bucketName=bucket_name, prefix=obs_path, marker=mark, max_keys=max_num, delimiter='/', encoding_type='url')
            if resp.status < 300:
                for content in resp.body.commonPrefixs:
                    result.append(content["prefix"])

                if resp.body.is_truncated is True:
                    mark = resp.body.next_marker
                else:
                    break
            else:
                raise Exception("List Object Failed, errorCode:{},errorMessage:{}".format(resp.errorCode, resp.errorMessage))
        return result
                
    def download_object(self, obs_path, local_path):
        try:
            self.obs_client.getObject(bucketName=self.input_bucket_name, objectKey=obs_path, downloadPath=local_path)
        except Exception as e:
            print(e)

    def upload_file(self, obs_path, local_path):
        try:
            self.obs_client.putFile(bucketName=self.output_bucket_name, objectKey=obs_path, file_path=local_path)
        except Exception as e:
            print(e)

    def upload_one(self, args_tuple):

        file, obs_path, local_path = args_tuple

        abst_obs_path = join_path(obs_path, file)

        self.obs_client.putFile(
            bucketName=self.output_bucket_name, 
            objectKey=abst_obs_path, 
            file_path=os.path.join(local_path, file)
        )


    def upload_folder(self, obs_path: str, local_path: str):
        """上传文件夹到指令路径"""
        # 列出文件夹内所有文件
        all_files_path = []
        for root, dirs, files in os.walk(local_path):
            for file in files:
                path_root = Path(root).resolve()
                path_local = Path(local_path).resolve()
                all_files_path.append((os.path.join(path_root.relative_to(path_local), file), obs_path, local_path))

        with concurrent.futures.ThreadPoolExecutor(max_workers=500) as executor:
            
            result = executor.map(self.upload_one, all_files_path)



        
# ps: 由于相对路径计算问题60b解压出来的pcd文件多了一层.目录
if __name__ == '__main__':

    ak = os.getenv("ak")
    sk = os.getenv("sk")
    endpoint = os.getenv("endpoint")
    input_bucket_name = os.getenv("input_bucket_name")
    input_obs_path = os.getenv("input_obs_path")
    output_bucket_name = os.getenv("output_bucket_name")
    output_obs_path = os.getenv("output_obs_path")

    RAW_PATH_DATE = input_obs_path
    TARGET_PATH = output_obs_path
    LOCAL_PATH = r"/home"
    con_obs_client = OBSClient(
        ak=ak, 
        sk=sk, 
        endpoint=endpoint,
        input_bucket_name=input_bucket_name,
        input_obs_path=input_obs_path,
        output_bucket_name=output_bucket_name,
        output_obs_path=output_obs_path
    )

    print(get_current_time(), "handle start")

    # 处理camera和lidar任务中的压缩包
    task_name_list = [os.path.basename(str(x).removesuffix("/")) for x in con_obs_client.list_sub_dir(bucket_name=input_bucket_name, obs_path=join_path(RAW_PATH_DATE, "bag"))]
    
    for task_index, one_task_name in enumerate(task_name_list):

        print(get_current_time(), "handling task: ", one_task_name, "    "+str(task_index)+"/"+str(len(task_name_list)))
        
        # 处理camera任务
        if one_task_name.startswith('78d'):

            print("handling 78d task:", one_task_name)    
            raw_camera_base_path = join_path(RAW_PATH_DATE, "bag", one_task_name)
            target_camera_base_path = join_path(TARGET_PATH, "camera")
            
            # 寻找压缩包
            zip_list = []
            tmp = con_obs_client.list_sub_dir(bucket_name=input_bucket_name, obs_path=raw_camera_base_path)
            for one in tmp:
                tmp02 = con_obs_client.list_object(bucket_name=input_bucket_name, obs_path=join_path(one, "record", "extracted"))
                for one02 in tmp02:
                    if str(one02).endswith(".zip"):
                        zip_list.append(one02)

            # 处理压缩包
            for index_78d, zip_abst_path in enumerate(zip_list):
                    
                print(get_current_time(), "handling zip file:", os.path.basename(zip_abst_path), "    "+str(index_78d)+"/"+str(len(zip_list)))
                os.makedirs(os.path.join(LOCAL_PATH, "zip"), exist_ok=True)
                os.makedirs(os.path.join(LOCAL_PATH, "extracted"), exist_ok=True)
                # 下载到本地
                con_obs_client.download_object(obs_path=zip_abst_path, local_path=os.path.join(LOCAL_PATH, "zip", os.path.basename(zip_abst_path)))
                # 本地解压
                with zipfile.ZipFile(os.path.join(LOCAL_PATH, "zip", os.path.basename(zip_abst_path)), 'r') as zip_ref:
                    zip_ref.extractall(os.path.join(LOCAL_PATH, "extracted"))
                # 上传到云道
                con_obs_client.upload_folder(
                    obs_path=join_path(target_camera_base_path, str(os.path.basename(zip_abst_path)).removesuffix('.zip')), 
                    local_path=join_path(LOCAL_PATH, "extracted")
                )
                # 删除本地压缩包和解压文件
                shutil.rmtree(os.path.join(LOCAL_PATH, "zip"))
                shutil.rmtree(os.path.join(LOCAL_PATH, "extracted"))


        # 处理lidar任务
        elif one_task_name.startswith('60b'):
            
            print("handling 60b task:", one_task_name)    
            raw_lidar_base_path = join_path(RAW_PATH_DATE, "bag", one_task_name)
            target_lidar_base_path = join_path(TARGET_PATH, "lidar")
            
            # 寻找压缩包
            zip_list = []
            tmp = con_obs_client.list_sub_dir(bucket_name=input_bucket_name, obs_path=raw_lidar_base_path)
            for one in tmp:
                tmp02 = con_obs_client.list_object(bucket_name=input_bucket_name, obs_path=join_path(one, "rosbag", "extracted"))
                for one02 in tmp02:
                    if str(one02).endswith(".zip"):
                        zip_list.append(one02)

            # 处理压缩包
            for index_60b, zip_abst_path in enumerate(zip_list):
                
                print(get_current_time(), "handling zip file:", os.path.basename(zip_abst_path), "    "+str(index_60b)+"/"+str(len(zip_list)))
                os.makedirs(os.path.join(LOCAL_PATH, "zip"), exist_ok=True)
                os.makedirs(os.path.join(LOCAL_PATH, "extracted"), exist_ok=True)
                # 下载到本地
                con_obs_client.download_object(obs_path=zip_abst_path, local_path=os.path.join(LOCAL_PATH, "zip", os.path.basename(zip_abst_path)))
                # 本地解压
                with zipfile.ZipFile(os.path.join(LOCAL_PATH, "zip", os.path.basename(zip_abst_path)), 'r') as zip_ref:
                    zip_ref.extractall(os.path.join(LOCAL_PATH, "extracted"))
                # 以timestamp重命名
                if not os.path.exists(os.path.join(LOCAL_PATH, "extracted", "total_frame.txt")):
                    # 删除本地压缩包和解压文件
                    shutil.rmtree(os.path.join(LOCAL_PATH, "zip"))
                    shutil.rmtree(os.path.join(LOCAL_PATH, "extracted"))
                    continue
                with open(os.path.join(LOCAL_PATH, "extracted", "total_frame.txt"), 'r') as file:
                    lidar_map_lines = file.readlines()
                lidar_timestamp_map = {}
                for one_line in lidar_map_lines:
                    one_line = one_line.strip()
                    parts = one_line.split()
                    value, key = parts[:2]
                    key = str(key)
                    value = str(value).replace(".", "", 1)
                    lidar_timestamp_map[key] = value
                if lidar_timestamp_map.get("lidar_frame"):
                    del lidar_timestamp_map["lidar_frame"]
                for key, value in lidar_timestamp_map.items():
                    try:
                        os.rename(os.path.join(LOCAL_PATH, "extracted", key+".pcd"), os.path.join(LOCAL_PATH, "extracted", str(value).ljust(19, "0")+".pcd"))
                    except Exception as e:
                        print("ERROR:", e)
                        continue

                # 上传到云道
                con_obs_client.upload_folder(
                    obs_path=join_path(target_lidar_base_path, str(os.path.basename(zip_abst_path)).removesuffix('.bag.zip')), 
                    local_path=join_path(LOCAL_PATH, "extracted")
                )
                # 删除本地压缩包和解压文件
                shutil.rmtree(os.path.join(LOCAL_PATH, "zip"))
                shutil.rmtree(os.path.join(LOCAL_PATH, "extracted"))
    print(get_current_time(), "all task done")
        
    










