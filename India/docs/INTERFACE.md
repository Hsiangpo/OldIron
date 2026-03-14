# 接口/数据结构说明

## 输出记录结构（JSONL）：每行是一条公司记录

```json
{
  "cin": "AAE-6126",
  "name": "PACIFIC INFRABUILD LLP",
  "status": "Active",
  "paid_up_capital": "0",
  "address": "Farm No. 10, North Drive...",
  "detail_url": "https://www.zaubacorp.com/PACIFIC-INFRABUILD-LLP-AAE-6126",
  "basic_info": {
    "LLP Identification Number": "AAE-6126",
    "ROC": "RoC-Delhi",
    "Date of Incorporation": "2015-08-24"
  },
  "contact_details": {
    "Email ID": "jainvarun23@hotmail.com",
    "Website": "Not Available",
    "Address": "Farm No. 10, North Drive..."
  },
  "current_director": {
    "DIN": "08056255",
    "Director Name": "HEMANT KUMAR SATIJA",
    "Designation": "Director",
    "Appointment Date": "2018-09-30"
  }
}
```

## 输出 CSV（基础字段 + JSON 字符串）

字段：
- `cin`
- `name`
- `status`
- `paid_up_capital`
- `address`
- `detail_url`
- `basic_info`（JSON 字符串）
- `contact_details`（JSON 字符串）
- `current_director`（JSON 字符串）
