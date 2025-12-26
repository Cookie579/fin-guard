"""
LLM Explainer Prompts
=====================

This module contains the hardcoded prompt templates used by the explainer service.
"""

SYSTEM_PROMPT = """You are a senior ESG Investment Analyst at a top-tier hedge fund (e.g., Bridgewater, BlackRock). 
Your job is to synthesize complex ESG, financial, and social sentiment data into a professional, actionable investment report.

Tone: Professional, data-driven, objective, yet insightful. Avoid generic fluff.
Format: GitHub-flavored Markdown. Use bolding for key metrics and insights.
"""

EXECUTIVE_SUMMARY_PROMPT = """
Context:
We are analyzing {company} ({ticker}).

Data Summary:
- ESG Score: {esg_score}
- Environmental Score: {e_score}
- Social Score: {s_score}
- Governance Score: {g_score}
- Recent 10-K Filing Date: {filing_date}
- Social Sentiment (StockTwits): {stocktwits_sentiment}
- Social Sentiment (YouTube): {youtube_sentiment}

Task:
Write a concise **Executive Summary** (150-200 words).
1. Start with a high-level verdict on the company's ESG standing.
2. Highlight the most critical risk or opportunity found in the data.
3. Briefly mention if social sentiment aligns with the fundamental ESG data.

Output Format:
## 🚀 Executive Summary
[Your text here]
"""

ESG_DEEP_DIVE_PROMPT = """
Context:
Deep dive into {company}'s ESG performance.

ESG Data:
{esg_details}

Topic Modeling Insights:
{topic_details}

Task:
Write a detailed **ESG Deep Dive** section (400-500 words).
1. **Environmental:** Analyze emissions, sustainability initiatives, and green tech adoption.
2. **Social:** Evaluate labor practices, diversity, and community impact.
3. **Governance:** Assess board structure, ethics, and transparency.
4. **Key Insight:** Use the topic modeling data to highlight specific themes (e.g., "supply chain", "renewable energy").

Output Format:
## 🌍 ESG Deep Dive
### Environmental (Score: {e_score})
[Analysis]

### Social (Score: {s_score})
[Analysis]

### Governance (Score: {g_score})
[Analysis]

> **💡 Analyst Insight:** [One unique insight based on the data]
"""

SEC_ANALYSIS_PROMPT = """
Context:
Analysis of {company}'s latest SEC 10-K filing.

SEC Data:
{sec_data}

Risk Factors:
{risk_factors}

Task:
Write an **SEC Filing Analysis** section (300-400 words).
1. **Financial Health:** Summarize key financial indicators mentioned in the text.
2. **Risk Assessment:** Analyze the top risk factors identified. Are they operational, regulatory, or market-based?
3. **Management Tone:** What is the overall tone of the filing? (Optimistic, cautious, defensive?)

Output Format:
## 📊 SEC Filing Insights (10-K)
[Analysis]

### ⚠️ Key Risk Factors
- **[Risk 1]:** [Description]
- **[Risk 2]:** [Description]
"""

SOCIAL_SENTIMENT_PROMPT = """
Context:
Social sentiment analysis for {company}.

StockTwits Data:
{stocktwits_data}

YouTube Data:
{youtube_data}

Task:
Write a **Social Sentiment Analysis** section (200-300 words).
1. **Retail Sentiment:** How are retail investors (StockTwits) reacting? Bullish or bearish?
2. **Media/Influencer Sentiment:** What is the vibe on YouTube?
3. **Divergence:** Is there a difference between retail sentiment and the official ESG/SEC data?

Output Format:
## 🗣️ Social Sentiment Analysis
| Platform | Sentiment Score | Volume |
|----------|----------------|--------|
| StockTwits | {stocktwits_score} | {stocktwits_vol} |
| YouTube | {youtube_score} | {youtube_vol} |

[Analysis text]
"""

RECOMMENDATION_PROMPT = """
Context:
Final investment recommendation for {company}.

Summary of Findings:
- ESG: {esg_summary}
- SEC: {sec_summary}
- Social: {social_summary}

Task:
Write a **Final Recommendation** section (150-200 words).
1. **Verdict:** Buy, Hold, or Sell (from an ESG perspective).
2. **Actionable Advice:** What should the investor monitor next?
3. **Catalysts:** What upcoming events could change this score?

Output Format:
## 🎯 Final Recommendation
**Verdict:** [Verdict]

[Analysis]

### 🔍 What to Watch
1. [Item 1]
2. [Item 2]
"""
