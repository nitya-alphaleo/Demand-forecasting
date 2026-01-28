from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import requests
import os
import json
import time
from datetime import datetime
import traceback

app = Flask(__name__)
CORS(app) 
# Enable CORS for all routes

# =============================
# Databricks Configuration
# =============================
HOST = os.getenv("DATABRICKS_HOST", "https://dbc-69ad6c24-a9b6.cloud.databricks.com")
TOKEN = os.getenv("DATABRICKS_TOKEN", "DATABRICKS_TOKEN")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID") or os.getenv("DATABRICKS_GENIE_ID", "DATABRICKS_GENIE_ID")

# **What's happening:**
# - **Reading configuration from environment variables**
# - `HOST`: Your Databricks workspace URL
# - `TOKEN`: Authentication token to access Databricks
# - `GENIE_SPACE_ID`: Which Genie space to use (contains your data schema)

# Validate configuration
if TOKEN == "DATABRICKS_TOKEN" or GENIE_SPACE_ID == "DATABRICKS_GENIE_ID" or not GENIE_SPACE_ID:
    print("\n" + "="*60)
    print("⚠️  WARNING: Using placeholder credentials!")
    print("="*60)
    print("Please set environment variables:")
    print("  Windows PowerShell:")
    print(" $env:DATABRICKS_TOKEN='your_token'")
    print(" $env:DATABRICKS_GENIE_ID='your_space_id'")
    print("="*60 + "\n")

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

#Creating HTTP headers for API authentication

# =============================
# HELPER FUNCTIONS (MUST COME FIRST!)
# =============================
def make_request(method, url, **kwargs):
    """Make HTTP request with error handling"""
    try:
        kwargs.setdefault('timeout', 10)
        kwargs.setdefault('headers', headers)
        
        response = requests.request(method, url, **kwargs)
        
        # Log request details for debugging
        print(f"\n{'='*60}")
        print(f"🌐 {method.upper()} {url}")
        print(f"📊 Status: {response.status_code}")
        
        return response
# Wrapper function to make HTTP requests safer
# Automatically adds authentication headers
        
    except requests.exceptions.Timeout:
        print(f"⏱️ Timeout connecting to {url}")
        raise
    except requests.exceptions.ConnectionError:
        print(f"🔌 Connection error to {url}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        raise

def get_dashboard_details(dashboard_id):
    """Fetch dashboard configuration and widgets (modern Lakeview endpoint)"""
    try:
        url = f"{HOST}/api/2.0/lakeview/dashboards/{dashboard_id}"
        response = make_request('GET', url)
        
        if response.status_code == 200:
            print("Dashboard fetched successfully")
            return response.json()
        else:
            print(f"❌ Dashboard fetch failed: {response.text[:300]}")
            return None
            
    except Exception as e:
        print(f"❌ Error in get_dashboard_details: {str(e)}")
        return None
# Fetches dashboard configuration from Databricks
# Used to display visualizations on your webpage

def get_query_results(query_id):
    """Fetch query execution results"""
    try:
        url = f"{HOST}/api/2.0/sql/queries/{query_id}/results"
        response = make_request('GET', url)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Query results fetch failed: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error in get_query_results: {str(e)}")
        return None
# Gets results from a SQL query that was already executed
# Used by dashboard feature

def call_genie_api(question):
    """
    Send a question to Databricks Genie and return the response.
    """
    try:
        # Start conversation
        global headers  # ← add this line
        start_url = f"{HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation"
        start_payload = {"content": question}

        start_response = requests.post(start_url, headers=headers, json=start_payload, timeout=10)
        if start_response.status_code != 200:
            return {
                "success": False,
                "error": f"Failed to start conversation. Status: {start_response.status_code}, Response: {start_response.text}"
            }

        data = start_response.json()
        conversation_id = data.get("conversation_id")
        message_id = data.get("message_id")
        if not conversation_id or not message_id:
            return {
                "success": False,
                "error": "Missing conversation_id or message_id in Genie response."
            }

        # Poll Genie for completion
        status_url = f"{HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}"
        
        for attempt in range(1, 31):
            time.sleep(2)
            print(f"Poll attempt {attempt}/30 for question: {question}")
            
            status_response = requests.get(status_url, headers=headers, timeout=10)
            if status_response.status_code != 200:
                print(f"  → Status not 200: {status_response.status_code}")
                continue

            status_data = status_response.json()
            status = status_data.get("status", "")
            print(f"  → Status: {status}")
            
            if status == "COMPLETED":
                print("\n===== GENIE RESPONSE COMPLETED =====")
                
                parts = []
                
                # 1. Direct message content (rare)
                msg_content = status_data.get("message", {}).get("content", "").strip()
                if msg_content:
                    parts.append(msg_content)
                
                # 2. Attachments – FIXED extraction
                attachments = status_data.get("attachments", [])
                print(f"Found {len(attachments)} attachments")
                
                for idx, att in enumerate(attachments, 1):
                    # Debug: show attachment structure
                    print(f"  Attachment {idx}: {json.dumps(att, indent=2)[:300]}...")
                    
                    # FIXED: Directly check for "text" key (no need for "type")
                    if "text" in att and isinstance(att["text"], dict):
                        txt = att["text"].get("content", "").strip()
                        if txt:
                            parts.append(txt)
                            print(f"    → Extracted text content ({len(txt)} chars)")
                            continue  # found it, skip other checks
                    
                    # Optional: query handling (SQL + results)
                    if "query" in att:
                        query_info = att["query"]
                        sql = query_info.get("query", "").strip()
                        if sql:
                            parts.append(f"Generated SQL:\n{sql}\n")
                        
                        attachment_id = att.get("attachment_id")
                        if attachment_id:
                            result_url = f"{HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result"
                            try:
                                result_resp = requests.get(result_url, headers=headers, timeout=15)
                                if result_resp.status_code == 200:
                                    result_data = result_resp.json()
                                    rows = result_data.get("result", []) or result_data.get("data", [])
                                    if rows:
                                        if isinstance(rows[0], dict):
                                            headers = list(rows[0].keys())
                                            header_line = " | ".join(headers)
                                            sep = "---|" * len(headers)
                                            lines = [header_line, sep]
                                            for row in rows[:10]:
                                                lines.append(" | ".join(str(row.get(k, "")) for k in headers))
                                            table_md = "\n".join(lines)
                                            parts.append(f"Query Results:\n{table_md}")
                                        else:
                                            parts.append(f"Raw rows:\n{json.dumps(rows, indent=2)}")
                                    else:
                                        parts.append("No rows in query result.")
                                else:
                                    print(f"  Query result fetch failed: {result_resp.status_code}")
                            except Exception as e:
                                print(f"  Error fetching query result: {e}")
                    
                    # Suggested questions
                    if "suggested_questions" in att:
                        questions = att["suggested_questions"].get("questions", [])
                        if questions:
                            parts.append("Suggested follow-up questions:\n" + "\n".join(f"- {q}" for q in questions))
                
                # Final combined answer
                # Final combined answer – prefer the clean text summary
                full_answer = ""

                # Look for the best text part (usually the last "text" attachment is the summary)
                for p in reversed(parts):  # start from end – summary is often last
                    if p.strip() and len(p) > 50 and "**" in p:  # look for bold text, typical for final answer
                        full_answer = p.strip()
                        break

                # If no good summary found, fall back to first useful text
                if not full_answer:
                    for p in parts:
                        if p.strip() and len(p) > 50:
                            full_answer = p.strip()
                            break

                # Very last fallback
                if not full_answer:
                    full_answer = "Genie returned data, but no clean summary was found."

                print("\n===== CLEAN FINAL ANSWER SENT TO FRONTEND =====")
                print(full_answer)
                print("=======================================\n")

                
                return {"success": True, "answer": full_answer}

            elif status in ["FAILED", "CANCELLED", "CANCELED"]:
                error_msg = status_data.get("error", {}).get("message", "Query failed without details")
                print(f"Query failed: {error_msg}")
                return {"success": False, "error": error_msg}

        return {"success": False, "error": "Query timed out after 60 seconds."}

    except Exception as e:
        traceback.print_exc()
        return {"success": False, "error": f"Unexpected error: {str(e)}"}

# =============================
# Routes
# =============================
@app.route("/")
def index():
    #Render main dashboard UI
    return render_template("index.html")

@app.route("/dashboard.html")
def dashboard1():
    return render_template("dashboard.html")

@app.route("/second_dashboard.html")
def dashboard2():
    return render_template("second_dashboard.html")


@app.route("/api/visualizations/<dashboard_id>")
def get_visualizations(dashboard_id):
    """Fetch all visualizations from a dashboard"""
    dashboard = get_dashboard_details(dashboard_id)
    
    if not dashboard:
        return jsonify({
            "error": "Failed to load dashboard",
            "details": "Check console for details"
        }), 500

    visualizations = []
    # Modern Lakeview: widgets are inside parsed serialized_dashboard
    parsed = dashboard.get("serialized_dashboard", "{}")
    try:
        parsed_def = json.loads(parsed) if parsed else {}
        pages = parsed_def.get("pages", [])
        for page in pages:
            widgets = page.get("widgets", [])
            for widget in widgets:
                viz = widget.get("visualization") or widget.get("viz")
                if not viz:
                    continue
                query_id = viz.get("query", {}).get("id") or viz.get("queryId")
                if query_id:
                    results = get_query_results(query_id)
                    if results:
                        visualizations.append({
                            "title": widget.get("title") or widget.get("text", "Untitled"),
                            "type": viz.get("type", "table"),
                            "data": results
                        })
    except Exception as e:
        print("Error parsing dashboard:", str(e))

    if not visualizations:
        print("No visualizations found in dashboard")

    return jsonify(visualizations)

@app.route("/api/chat", methods=["POST"])
def chat():
    
    # Chat endpoint:
    # - Receives user question
    # - Sends to Genie
    # - Returns response
    
    try:
        data = request.get_json()
        if not data:
            return jsonify({"answer": "Invalid request format", "success": False}), 400
        
        question = data.get("question", "").strip()
        if not question:
            return jsonify({"answer": "Please ask a question.", "success": False})
        
        # Handle greetings locally
        greetings = ["hi", "hello", "hey", "hi!", "hello!", "hey!", "hi there"]
        if question.lower() in greetings:
            return jsonify({
                "answer": (
                    "Hi! 👋 I'm your Genie Assistant powered by Databricks.\n\n"
                    "I can help you analyze your drug demand forecasting data. Try asking:\n\n"
                    "• What are the highest disease cases in Karnataka?\n"
                    "• Which disease has the maximum cases?\n"
                    "• Show me disease trends over time\n"
                    "• Compare cases by district\n"
                    "• What's the total count for a specific disease?"
                ),
                "success": True
            })
        
        # Call Databricks Genie API
        genie_response = call_genie_api(question)
        
        if genie_response["success"]:
            answer_text = genie_response.get("answer", "Genie did not return any answer.")
            
            response_data = {
                "answer": answer_text,
                "success": True
            }
            
            return jsonify(response_data)
        else:
            return jsonify({
                "answer": f"⚠️ {genie_response.get('error', 'An error occurred')}\n\n"
                          "💡 Try asking about diseases, cases, or trends in your data.",
                "success": False
            })
    except Exception as e:
        print(f"❌ Error in /api/chat: {str(e)}")
        traceback.print_exc()
        return jsonify({"answer": f"Server error: {str(e)}", "success": False}), 500

@app.route("/api/health")
def health_check():
    """Health check endpoint with configuration info"""
    config_valid = (
        TOKEN != "DATABRICKS_TOKEN" and
        GENIE_SPACE_ID != "DATABRICKS_GENIE_ID"
    )
    
    return jsonify({
        "status": "healthy",
        "host": HOST,
        "genie_space_id": GENIE_SPACE_ID[:8] + "..." if len(GENIE_SPACE_ID) > 8 else GENIE_SPACE_ID,
        "config_valid": config_valid,
        "token_set": TOKEN != "DATABRICKS_TOKEN"
    })


# =============================
# Error Handlers
# =============================
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Internal server error", "details": str(e)}), 500

# =============================
# Main
# =============================
if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 DATABRICKS GENIE CHATBOT APPLICATION")
    print("="*60)
    print(f"📍 Server: http://localhost:5000")
    print(f"🔗 Host: {HOST}")
    print(f"🧞 Genie Space: {GENIE_SPACE_ID[:20]}...")
    print(f"🔑 Token: {'✅ Set' if TOKEN != 'DATABRICKS_TOKEN' else '❌ Not set'}")
    print("="*60)
    
    if TOKEN == "DATABRICKS_TOKEN" or GENIE_SPACE_ID == "DATABRICKS_GENIE_ID":
        print("\n⚠️  Configuration needed! Run this first:")
        print("   set DATABRICKS_TOKEN=your_token")
        print("   set GENIE_SPACE_ID=your_space_id")
        print()
    
    print("✅ Server starting...\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)