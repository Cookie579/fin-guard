"""
The Investment Committee (The Decision Maker)
=============================================

Simulates a debate between three AI agents:
1. The Bull (Growth focused)
2. The Bear (Risk focused)
3. The Risk Officer (Moderator)
"""

from typing import Dict, List
import json

class InvestmentCommittee:
    def __init__(self, llm_client, model: str):
        self.client = llm_client
        self.model = model

    def conduct_debate(self, ticker: str, context: str) -> Dict[str, str]:
        """
        Run the multi-agent debate.
        context: Aggregated data from RAG, Market, and Competitor analysis.
        """
        print(f"\n🏛️  Convening Investment Committee for {ticker}...")

        # 1. The Bull's Opening Statement
        bull_prompt = f"""
        You are 'The Bull', an aggressive growth investor.
        Analyze this data for {ticker} and make a case for a STRONG BUY.
        Focus on moats, growth catalysts, and upside potential. Ignore the risks.
        
        CRITICAL: Provide a complete, final analysis ONLY. Do NOT include:
        - Follow-up questions or offers like "If you'd like, I can..."
        - Suggestions to provide more analysis
        - Any prompts asking what else to cover
        Your response must be a definitive statement that ends conclusively.
        
        Data:
        {context}
        """
        bull_case = self._call_llm(bull_prompt, "You are a bullish hedge fund analyst.")
        print("  🐂 Bull: Case made.")

        # 2. The Bear's Rebuttal
        bear_prompt = f"""
        You are 'The Bear', a skeptical short-seller.
        Read the Bull's case and the data for {ticker}.
        Tear it apart. Focus on valuation, fraud risks, and macro headwinds.
        Make a case for SELL.
        
        CRITICAL: Provide a complete, final analysis ONLY. Do NOT include:
        - Follow-up questions or offers like "If you'd like, I can..."
        - Suggestions to provide more analysis
        - Any prompts asking what else to cover
        Your response must be a definitive statement that ends conclusively.
        
        Bull's Case:
        {bull_case}
        
        Data:
        {context}
        """
        bear_case = self._call_llm(bear_prompt, "You are a bearish short-seller.")
        print("  🐻 Bear: Rebuttal delivered.")

        # 3. The Risk Officer's Verdict
        risk_prompt = f"""
        You are the Chief Risk Officer.
        Review the Bull and Bear arguments for {ticker}.
        Issue a final binding verdict (BUY, HOLD, or SELL).
        Explain your reasoning, weighing the upside vs. downside.
        
        CRITICAL: Provide a complete, final verdict ONLY. Do NOT include:
        - Follow-up questions or offers like "If you'd like, I can..."
        - Suggestions to provide more analysis
        - Any prompts asking what else to cover
        Your verdict must be definitive and end conclusively with clear action items.
        
        Bull: {bull_case}
        Bear: {bear_case}
        """
        verdict = self._call_llm(risk_prompt, "You are a conservative Chief Risk Officer.")
        print("  ⚖️  Risk Officer: Verdict issued.")

        return {
            "bull_case": bull_case,
            "bear_case": bear_case,
            "verdict": verdict
        }

    def _call_llm(self, prompt: str, system_role: str) -> str:
        import time
        
        # List of models to try in order
        backup_models = [
            "gpt-4o-mini",
            "gpt-4o",
            "gpt-4-turbo",
            "gpt-4"
        ]
        
        # Create full list: [current_model, *backups]
        # Remove duplicates while preserving order
        candidate_models = []
        if self.model not in candidate_models:
            candidate_models.append(self.model)
        for m in backup_models:
            if m != self.model:
                candidate_models.append(m)
                
        last_error = None

        for model_name in candidate_models:
            try:
                # Rate limiting: 10 RPM = 6 seconds between calls
                # We do this check before every attempt
                if hasattr(self, '_last_call_time'):
                    elapsed = time.time() - self._last_call_time
                    if elapsed < 6:
                        time.sleep(6 - elapsed)
                
                self._last_call_time = time.time()
                
                print(f"  🤖 Querying {model_name}...")
                
                # Use OpenAI chat completions with proper message structure
                response = self.client.chat.completions.create(
                    model=model_name,
                    messages=[
                        {"role": "system", "content": system_role},
                        {"role": "user", "content": prompt}
                    ]
                )
                return response.choices[0].message.content
                
            except Exception as e:
                last_error = e
                error_str = str(e)
                # Check for rate limit errors
                if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str:
                    print(f"  ⚠️  Rate limit hit on {model_name}. Switching model...")
                    time.sleep(2) # Brief pause before switching
                    continue
                else:
                    # Non-rate-limit error, likely fatal for this request
                    print(f"  ❌ Error on {model_name}: {e}")
                    return f"(Agent Error: {e})"
        
        # If we get here, all models failed
        return f"(Agent Error: All models exhausted. Last error: {last_error})"

