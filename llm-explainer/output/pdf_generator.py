"""
PDF Generator (The Publisher)
=============================

Compiles analysis into a professional, recruiter-impressive PDF report.
Uses Playwright for Windows-compatible PDF generation.
Features modern typography, smaller fonts, and optimized layout.
"""

from pathlib import Path
import markdown
import tempfile

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    print("⚠️  Playwright missing. Install: pip install playwright && playwright install chromium")
    PLAYWRIGHT_AVAILABLE = False

class PDFGenerator:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_report(self, ticker: str, markdown_content: str, charts: dict) -> Path:
        """
        Convert Markdown to PDF with embedded charts using Playwright.
        """
        if not PLAYWRIGHT_AVAILABLE:
            print("⚠️  Skipping PDF generation - Playwright not available")
            return None

        # Convert MD to HTML
        html_body = markdown.markdown(markdown_content, extensions=['tables', 'fenced_code'])
        
        # Inject Charts
        for name, path in charts.items():
            # Convert Windows path to file:/// URL format
            file_url = Path(path).as_posix()
            if not file_url.startswith('file:///'):
                file_url = f"file:///{file_url}"
            
            img_tag = f'<img src="{file_url}" class="chart" alt="{name}">'
            # Simple replacement placeholder logic
            if f"{{{{ {name} }}}}" in html_body:
                html_body = html_body.replace(f"{{{{ {name} }}}}", img_tag)
            else:
                # Append if not found (for new charts)
                section_title = name.replace('_', ' ').title()
                html_body += f"\n<div class='chart-section'><h3>{section_title}</h3>{img_tag}</div>"

        # Professional HTML Template with modern styling
        full_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{ticker} Investment Report | FinGuard AI</title>
            <meta charset="UTF-8">
            <link rel="preconnect" href="https://fonts.googleapis.com">
            <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
            <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
            <style>
                /* Reset and Base */
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                
                body {{
                    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    font-size: 12px;
                    line-height: 1.5;
                    color: #2d3748;
                    background: #ffffff;
                    max-width: 100%;
                    margin: 0;
                    padding: 25px 30px;
                }}
                
                /* Typography */
                h1 {{
                    font-size: 22px;
                    font-weight: 700;
                    color: #1a365d;
                    border-bottom: 3px solid #1a365d;
                    padding-bottom: 8px;
                    margin-bottom: 20px;
                    margin-top: 0;
                }}
                
                h2 {{
                    font-size: 16px;
                    font-weight: 600;
                    color: #2c5282;
                    margin-top: 25px;
                    margin-bottom: 12px;
                    padding-bottom: 5px;
                    border-bottom: 1px solid #e2e8f0;
                }}
                
                h3 {{
                    font-size: 12px;
                    font-weight: 600;
                    color: #4a5568;
                    margin-top: 15px;
                    margin-bottom: 8px;
                }}
                
                p {{
                    margin-bottom: 10px;
                    text-align: justify;
                }}
                
                /* Lists */
                ul, ol {{
                    margin-left: 20px;
                    margin-bottom: 12px;
                }}
                
                li {{
                    margin-bottom: 4px;
                    line-height: 1.4;
                }}
                
                /* Emphasis */
                strong {{
                    font-weight: 600;
                    color: #1a365d;
                }}
                
                /* Charts */
                .chart, img {{
                    max-width: 100%;
                    height: auto;
                    margin: 15px 0;
                    border-radius: 8px;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
                }}
                
                .chart-section {{
                    margin: 20px 0;
                    page-break-inside: avoid;
                }}
                
                /* Tables */
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin: 15px 0;
                    font-size: 9px;
                }}
                
                th, td {{
                    border: 1px solid #e2e8f0;
                    padding: 8px 10px;
                    text-align: left;
                }}
                
                th {{
                    background: linear-gradient(180deg, #f7fafc 0%, #edf2f7 100%);
                    font-weight: 600;
                    color: #2c5282;
                    text-transform: uppercase;
                    font-size: 8px;
                    letter-spacing: 0.5px;
                }}
                
                tr:nth-child(even) {{
                    background-color: #f7fafc;
                }}
                
                tr:hover {{
                    background-color: #edf2f7;
                }}
                
                /* Blockquotes */
                blockquote {{
                    border-left: 4px solid #e53e3e;
                    padding: 10px 15px;
                    margin: 15px 0;
                    background: #fff5f5;
                    border-radius: 0 6px 6px 0;
                    color: #742a2a;
                    font-style: italic;
                }}
                
                /* Code */
                code {{
                    background-color: #edf2f7;
                    padding: 2px 6px;
                    border-radius: 4px;
                    font-family: 'Consolas', 'Monaco', monospace;
                    font-size: 9px;
                }}
                
                pre {{
                    background-color: #2d3748;
                    color: #e2e8f0;
                    padding: 15px;
                    border-radius: 6px;
                    overflow-x: auto;
                    font-size: 9px;
                }}
                
                /* Header */
                .header {{
                    text-align: center;
                    margin-bottom: 30px;
                    padding-bottom: 20px;
                    border-bottom: 2px solid #e2e8f0;
                }}
                
                .logo {{
                    font-size: 14px;
                    font-weight: 700;
                    color: #38a169;
                    letter-spacing: 1px;
                    text-transform: uppercase;
                    margin-bottom: 5px;
                }}
                
                .subtitle {{
                    font-size: 10px;
                    color: #718096;
                    margin-top: 5px;
                }}
                
                /* Verdict Box */
                .verdict-box {{
                    background: linear-gradient(135deg, #1a365d 0%, #2c5282 100%);
                    color: white;
                    padding: 15px 20px;
                    border-radius: 8px;
                    margin: 15px 0;
                }}
                
                .verdict-box strong {{
                    color: #90cdf4;
                }}
                
                /* Section Dividers */
                hr {{
                    border: none;
                    height: 1px;
                    background: linear-gradient(90deg, transparent, #e2e8f0, transparent);
                    margin: 25px 0;
                }}
                
                /* Page breaks */
                .page-break {{
                    page-break-before: always;
                }}
                
                /* Footer */
                .footer {{
                    margin-top: 30px;
                    padding-top: 15px;
                    border-top: 1px solid #e2e8f0;
                    text-align: center;
                    font-size: 8px;
                    color: #a0aec0;
                }}
                
                /* Risk indicator colors */
                .risk-high {{ color: #e53e3e; font-weight: 600; }}
                .risk-medium {{ color: #d69e2e; font-weight: 600; }}
                .risk-low {{ color: #38a169; font-weight: 600; }}
                
                /* Print optimization */
                @media print {{
                    body {{
                        padding: 0;
                    }}
                    .chart-section {{
                        page-break-inside: avoid;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <div class="logo">🛡️ FinGuard AI Analyst</div>
                <h1>Investment Report: {ticker}</h1>
                <div class="subtitle">AI-Powered ESG & Financial Analysis | Generated {self._get_timestamp()}</div>
            </div>
            {html_body}
            <div class="footer">
                <p>This report was generated by FinGuard AI Analyst. For informational purposes only. Not financial advice.</p>
                <p>© {self._get_year()} FinGuard | Powered by Advanced AI Analysis</p>
            </div>
        </body>
        </html>
        """

        # Save HTML to temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
            f.write(full_html)
            html_path = f.name

        try:
            # Generate PDF using Playwright
            with sync_playwright() as p:
                browser = p.chromium.launch()
                page = browser.new_page()
                page.goto(f"file:///{Path(html_path).as_posix()}")
                
                # Wait for fonts to load
                page.wait_for_load_state('networkidle')
                
                output_path = self.output_dir / f"{ticker}_Report.pdf"
                page.pdf(
                    path=str(output_path),
                    format='A4',
                    margin={'top': '15mm', 'right': '15mm', 'bottom': '15mm', 'left': '15mm'},
                    print_background=True,
                    display_header_footer=False,
                )
                browser.close()
                
            print(f"📄 PDF Report saved to: {output_path}")
            return output_path
        finally:
            # Clean up temp HTML file
            Path(html_path).unlink(missing_ok=True)
    
    def _get_timestamp(self) -> str:
        """Get formatted timestamp for report."""
        from datetime import datetime
        return datetime.now().strftime("%B %d, %Y at %H:%M")
    
    def _get_year(self) -> str:
        """Get current year for footer."""
        from datetime import datetime
        return datetime.now().strftime("%Y")
