import json
import os
import sys

CONFIG_FILE = "data/provider_config.json"

def main():
    print("--- 配置 Biying Licence ---")
    
    # Check if config exists
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except:
            data = {}
    else:
        data = {}

    current = data.get("biying_licence", "")
    if current:
        print(f"当前 Licence: {current}")
    else:
        print("当前未配置 Licence")
    
    # Input
    try:
        new_licence = input("请输入新的 Licence (回车跳过): ").strip()
    except KeyboardInterrupt:
        print("\n取消")
        sys.exit(0)

    if new_licence:
        data["biying_licence"] = new_licence
        
        # Ensure dir exists
        os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
        
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            print("✅ Licence 已更新并保存到 data/provider_config.json")
        except Exception as e:
            print(f"❌ 保存失败: {e}")
    else:
        print("未更改")

if __name__ == "__main__":
    main()