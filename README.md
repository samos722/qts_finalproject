# QTS Final Project (Draft)

Crypto Perpetual Futures Market Neutral Strategy

**Team**: Samos Zhu (12498135), Wenxin Chang (12497798), Henry Xu (12284734), Cody Torgovnik (12496679)

## 使用

```bash
pip install -r requirements.txt
doit              # 数据爬取 + 构建 panel
```

完成后 notebook 直接加载 `data/` 下的数据。

Pitchbook 转 PDF：Marp / pandoc / PowerPoint 导出 `pitchbook/pitchbook.md`

## 目录

```
qts_finalproject/
├── dodo.py
├── requirements.txt
├── README.md
├── src/
│   ├── config.py
│   ├── data_fetch.py
│   └── align.py
├── notebooks/
│   └── technical_study.ipynb
├── pitchbook/
│   └── pitchbook.md
└── data/          # doit 生成
```
