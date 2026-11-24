严格按给定 JSON schema 返回一个完整 JSON 对象，不输出 Markdown 或其他正文。
JSON 对象的顶层只有 draft 字段，以下字段全部填写在 draft 对象内。
JSON 结构示例：{"draft":{"findings":[],"points":[],"events":[],"judgments":[],"fields":[],"relations":[],"proposals":[],"gaps":[],"uncertainties":[],"completion_notes":"已检查指定范围，当前仍有资料需要核实。","complete":false}}。
所有事实写入 findings，每条使用独立 ref，并引用实际支持它的 chunk_ids。
每条 finding 保持一个明确对象和事实范围。芯片、模组、加速卡和软件配置分别说明；不为增加信息而附加本问题不需要且范围不同的配置条件。
conditions 只保存影响事实适用范围的产品、版本、测试和部署条件；来源名称、资料发布日期由证据元数据记录，不填入 conditions。
findings.kind 只能为“事实”“术语”“部署条件”“软件条件”“商业进展”。性能、接口、版本和产品规格均可归为“事实”，不得自行增加分类值。
把有价值且有原文依据的术语解释、产品条件、能力限制和行业发现保存为 findings，即使正文没有逐项讨论。
points 是给用户的回答，每条引用 finding_refs。只保留与问题相关的高价值结论，说明边界。
events 只记录有依据的行业进展；claim_refs 引用 findings.ref。完全相同的进展填写已知事件的 existing_event_id，并沿用对应 identity_key。
judgments 仅提出证据要求改变的专题判断；可以基于已有证据整理，不能将背景资料当作今日新闻。
已有知识包含相同事实时复用其精确表述与条件，补充新的来源；有变化时明确产品、版本、条件或结论的差异。
fields、relations、proposals 都必须使用 finding_ref 关联依据；缺少证据时留空。
gaps 提出能改变判断的具体后续问题，说明需要什么证据及完成标准；避免重复已解决问题。
相同后续问题已存在时填写 existing_gap_id，保留已有问题的状态与身份。
遵守 output_limits 指定的条目上限，optional_updates_total 是 events、judgments、fields、relations、proposals、gaps 合计上限。文字简洁，给出足够的产品、版本和测试条件。
没有依据的内容放 uncertainties；完成标准未满足时 complete=false。
