import pandas as pd
import time
import os
import requests
import sys
from datetime import datetime

def make_query(endpoint, params):
    """执行API查询"""
    url = f"https://api.ohsome.org/v1/{endpoint}"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    # 确保bcircles参数是列表格式
    if isinstance(params.get('bcircles'), str):
        params['bcircles'] = [params['bcircles']]
    
    max_retries = 3
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            response = requests.post(url, data=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"请求失败，{retry_delay}秒后重试...")
                time.sleep(retry_delay)
            else:
                print(f"请求失败: {str(e)}")
                if hasattr(e, 'response') and hasattr(e.response, 'text'):
                    print(f"错误详情: {e.response.text}")
                return None

def collect_airport_data(airport_name, iata, lat, lon, date):
    """收集单个机场的数据"""
    data = {
        "airport_name": airport_name,
        "iata": iata,
        "date": date.strftime("%Y-%m-%d")
    }
    
    # 构建不同范围的缓冲区
    buffer_3km = f"{lon},{lat},3000"    # 3km半径
    buffer_5km = f"{lon},{lat},5000"    # 5km半径
    buffer_10km = f"{lon},{lat},10000"  # 10km半径
    buffer_30km = f"{lon},{lat},30000"  # 30km半径
    buffer_100km = f"{lon},{lat},100000" # 100km半径
    
    # 1. 查询建筑物 (3km, 5km, 10km, 30km, 100km范围)
    for radius, buffer in [("3km", buffer_3km), ("5km", buffer_5km), ("10km", buffer_10km), 
                         ("30km", buffer_30km), ("100km", buffer_100km)]:
        building_params = {
            "bcircles": buffer,
            "time": date.strftime("%Y-%m-%d"),
            "filter": "building=* and building!=no"
        }
        building_result = make_query("elements/count", building_params)
        data[f"building_{radius}"] = building_result.get("result", [{"value": -1}])[0]["value"] if building_result else -1
    
    # 2. 查询办公楼 (3km, 5km, 10km, 30km, 100km范围)
    for radius, buffer in [("3km", buffer_3km), ("5km", buffer_5km), ("10km", buffer_10km), 
                         ("30km", buffer_30km), ("100km", buffer_100km)]:
        office_params = {
            "bcircles": buffer,
            "time": date.strftime("%Y-%m-%d"),
            "filter": "building=office"
        }
        office_result = make_query("elements/count", office_params)
        data[f"office_building_{radius}"] = office_result.get("result", [{"value": -1}])[0]["value"] if office_result else -1
    
    # 3. 查询火车站 (3km, 5km, 10km, 30km, 100km范围)
    for radius, buffer in [("3km", buffer_3km), ("5km", buffer_5km), ("10km", buffer_10km), 
                         ("30km", buffer_30km), ("100km", buffer_100km)]:
        train_params = {
            "bcircles": buffer,
            "time": date.strftime("%Y-%m-%d"),
            "filter": "railway=station and station!=subway and subway!=yes"
        }
        train_result = make_query("elements/count", train_params)
        data[f"train_station_{radius}"] = train_result.get("result", [{"value": -1}])[0]["value"] if train_result else -1
    
    # 4. 查询工业设施 (3km, 5km, 10km, 30km, 100km范围)
    for radius, buffer in [("3km", buffer_3km), ("5km", buffer_5km), ("10km", buffer_10km), 
                         ("30km", buffer_30km), ("100km", buffer_100km)]:
        industrial_params = {
            "bcircles": buffer,
            "time": date.strftime("%Y-%m-%d"),
            "filter": "building=industrial"
        }
        industrial_result = make_query("elements/count", industrial_params)
        data[f"industrial_facility_{radius}"] = industrial_result.get("result", [{"value": -1}])[0]["value"] if industrial_result else -1
    
    # 5. 查询旅游设施 (3km, 5km, 10km, 30km, 100km范围)
    for radius, buffer in [("3km", buffer_3km), ("5km", buffer_5km), ("10km", buffer_10km), 
                         ("30km", buffer_30km), ("100km", buffer_100km)]:
        tourism_params = {
            "bcircles": buffer,
            "time": date.strftime("%Y-%m-%d"),
            "filter": "tourism=*"
        }
        tourism_result = make_query("elements/count", tourism_params)
        data[f"tourism_facility_{radius}"] = tourism_result.get("result", [{"value": -1}])[0]["value"] if tourism_result else -1
    
    # 6. 查询商业设施 (3km, 5km, 10km, 30km, 100km范围)
    for radius, buffer in [("3km", buffer_3km), ("5km", buffer_5km), ("10km", buffer_10km), 
                         ("30km", buffer_30km), ("100km", buffer_100km)]:
        commercial_params = {
            "bcircles": buffer,
            "time": date.strftime("%Y-%m-%d"),
            "filter": "building=commercial"
        }
        commercial_result = make_query("elements/count", commercial_params)
        data[f"commercial_facility_{radius}"] = commercial_result.get("result", [{"value": -1}])[0]["value"] if commercial_result else -1
    
    # 7. 查询周围机场 (100公里范围，只统计有IATA的)
    airport_params = {
        "bcircles": buffer_100km,
        "time": date.strftime("%Y-%m-%d"),
        "filter": "aeroway=aerodrome and iata=*"
    }
    airport_result = make_query("elements/count", airport_params)
    airport_count = airport_result.get("result", [{"value": -1}])[0]["value"] if airport_result else -1
    # 减去自身
    data["surrounding_airport"] = max(0, airport_count - 1) if airport_count != -1 else -1
    
    # 8. 查询航站楼 (5公里范围)
    terminal_params = {
        "bcircles": buffer_5km,
        "time": date.strftime("%Y-%m-%d"),
        "filter": "aeroway=terminal"
    }
    terminal_result = make_query("elements/count", terminal_params)
    terminal_count = terminal_result.get("result", [{"value": -1}])[0]["value"] if terminal_result else -1
    # 如果航站楼数量为0，设置为1
    data["terminal"] = 1 if terminal_count == 0 else terminal_count
    
    # 9. 查询跑道 (5公里范围，只统计有width的)
    runway_params = {
        "bcircles": buffer_5km,
        "time": date.strftime("%Y-%m-%d"),
        "filter": "aeroway=runway and width=*"
    }
    runway_result = make_query("elements/count", runway_params)
    data["runway"] = runway_result.get("result", [{"value": -1}])[0]["value"] if runway_result else -1
    
    return data

def get_processed_airports(year):
    """获取已处理的机场列表"""
    output_dir = "historical_data"
    if not os.path.exists(output_dir):
        return set()
    
    processed = set()
    for file in os.listdir(output_dir):
        if file.endswith(f"_{year}.csv"):
            try:
                df = pd.read_csv(os.path.join(output_dir, file))
                processed.update(df['iata'].unique())
            except Exception as e:
                print(f"读取文件 {file} 时出错: {str(e)}")
    return processed

def main():
    if len(sys.argv) != 2:
        print("使用方法: python collect_single_year.py YYYY")
        sys.exit(1)
    
    try:
        year = int(sys.argv[1])
        if year < 2007 or year > 2024:
            print("年份必须在2007到2024之间")
            sys.exit(1)
    except ValueError:
        print("请提供有效的年份")
        sys.exit(1)
    
    # 设置目标日期为12月1日
    target_date = datetime(year, 12, 1)
    
    # 读取机场坐标
    try:
        airports_df = pd.read_csv("Airportlist.csv")
    except Exception as e:
        print(f"读取机场列表失败: {str(e)}")
        sys.exit(1)
    
    # 获取已处理的机场
    processed_airports = get_processed_airports(year)
    print(f"已处理的机场数量: {len(processed_airports)}")
    
    # 创建输出目录
    output_dir = "CHINA/historical_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # 处理每个机场
    results = []
    total_airports = len(airports_df)
    
    for index, row in airports_df.iterrows():
        iata = row['IATA']
        
        # 跳过已处理的机场
        if iata in processed_airports:
            print(f"跳过已处理的机场 {iata} ({index + 1}/{total_airports})")
            continue
        
        print(f"\n处理机场 {iata} ({index + 1}/{total_airports})")
        try:
            data = collect_airport_data(
                row['Airport name'],
                iata,
                row['Latitude'],
                row['Longitude'],
                target_date
            )
            results.append(data)
            print(f"完成机场 {iata} 的数据收集")
            
            # 每处理10个机场保存一次
            if len(results) % 10 == 0:
                save_results(results, output_dir, year)
                print(f"已保存 {len(results)} 个机场的数据")
            
        except Exception as e:
            print(f"处理机场 {iata} 时出错: {str(e)}")
            continue
    
    # 保存最终结果
    if results:
        save_results(results, output_dir, year)
        print(f"\n所有数据已保存到 {output_dir} 目录")
    else:
        print("\n没有新数据需要保存")

def save_results(results, output_dir, year):
    """保存结果到CSV文件"""
    if not results:
        return
        
    df = pd.DataFrame(results)
    output_file = os.path.join(output_dir, f"airport_data_{year}.csv")
    
    # 如果文件已存在，追加数据
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        df = pd.concat([existing_df, df], ignore_index=True)
        # 删除重复的机场数据
        df = df.drop_duplicates(subset=['iata', 'date'], keep='last')
    
    df.to_csv(output_file, index=False)

if __name__ == "__main__":
    main() 
