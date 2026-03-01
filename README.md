# KLine Resample Studio (1m ▷ 5m)

![KLine Resample Studio Preview](https://via.placeholder.com/800x500.png?text=KLine+Resample+Studio+-+Geek+Dark+Theme)

---

## 🇨🇳 中文说明 (Chinese)

**KLine Resample Studio** 是一套从 **KLine Matrix Station (1-Minute K-Line 数据下载器)** 衍生出的极客暗金风格桌面级数据转换终端。专门用于将 AA 股市场中颗粒细碎的 1 分钟级 K 线数据降维重组为标准的 5 分钟级 K 线，作为大模型预训练数据投喂前的预处理基座。

### ✨ 核心黑科技
- **[ 精准的熊猫重切片 (Pandas Resampling) ]**：基于 `pandas` DataFrame 技术，严格按照 A 股的标准（`closed='right', label='right'`）将 1 分钟细粒度数据聚合成 5 分钟的 OHLCV 形态，完美平滑切片边界，绝不漏斗数据。
- **[ 双轨交互视野 (Dual-Track Observer) ]**：搭载源文件扫描库（上）和结果输出陈列区（下）。全自动化同步，支持框选后的批量物理销毁，操作体验丝滑一致。
- **[ 动态绝对路径挂载 (Dynamic Absolute Mounting) ]**：程序自动推演运行目录并动态构建安全的绝对路径（自动创建缺少的子文件夹），这意味着无需人工配置，无论将整个端部署在系统任何层级，数据总能精准归位。
- **[ 极客暗金 UI ]**：沿用全系列高度统一的 Flat Dark Gold 骇客风格，带有系统运行日志面板（数据流控网络）不仅科技感拉满，同时抛弃了抽象的科幻术语，换以最直白的中文操作警示。

### 🚀 使用指南
1. **确保存盘前置**：先使用配对的工具 `1-KLine-Extract` 将全市场（或特定）标的下载并存放于公共的数据缓存目录下。
2. **点火执行**：在界面左侧框选全量或范围时间边界，单击【开始转换】。引擎将接管处理，生成附带 `_5m_` 后缀的新序列切片文件。
3. **输出流挂载**：输出文件夹将自动按需设立，结果直接落入硬盘可直接喂给后续大模型训练队列引擎。

---

## 🇺🇸 English Documentation

**KLine Resample Studio** is a geek-dark styled data transformation terminal naturally evolved from the **KLine Matrix Station**. It focuses on resampling fine-grained 1-minute financial K-Line data into 5-minute standard intervals, acting as the preprocessing core base before feeding data into a foundation LLM setup (like Kronos).

### ✨ Core Features
- **[ Precision Pandas Resampling ]**: Powered by `pandas` DataFrame `resample` logic. Strictly adheres to Chinese A-Share market rules (`closed='right', label='right'`) to aggregate 1-minute OHLCV data into perfectly aligned 5-minute segments without data leaks.
- **[ Dual-Track Observer UI ]**: Features an intuitive source scanning library at the top and the output payload archive at the bottom. Syncs automatically and supports bulk UI-based physical file deletion to streamline dataset management.
- **[ Dynamic Absolute Pathing ]**: Intelligently auto-deduces its execution ecosystem to build absolute resilient data paths. Run the suite anywhere globally on your filesystem, and data will natively hook and sink without configuration errors.
- **[ Geek Dark Theme ]**: Employs the same highly immersive Flat Dark Gold (Cyber-Gold) UI layout. Emphasizes clean UI telemetry logging while using easy-to-understand labels for bullet-proof UX execution.

---
`Author: Ziqi` | `License: MIT` | `Design Language: Cyber-Gold`
