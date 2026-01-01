# 🚀 Deployment Guide: Financial Analyst Agent

This guide will help you get the **AI Financial Analyst** running on your machine or server in under 5 minutes.

## 📋 Prerequisites
- **Docker Desktop** (or Docker Engine on Linux)
- **Google Gemini API Key** (Get it [here](https://aistudio.google.com/))
- **(Optional) Slack/Discord Webhook** for alerts.

---

## 🛠️ Step 1: Configuration

Create a file named `.env` in the project root folder. Copy the following content into it:

```bash
# Required: Your Gemini API Key
GOOGLE_API_KEY="AIzaSy..."

# Required: Your Name/Email for SEC Compliance (They require a user agent)
SEC_IDENTITY="Your Name your.email@example.com"

# Optional: Notification Webhook (Slack or Discord)
# Leave blank to just see logs in the terminal
NOTIFICATION_WEBHOOK_URL="https://discord.com/api/webhooks/..."
```

---

## 🐳 Step 2: Run with Docker (Recommended)

This is the easiest way. It runs both the **Agent** (background worker) and **Dashboard** (web app) automatically.

1. **Open your terminal** in the project folder.
2. **Run the build & start command**:
   ```bash
   docker-compose up --build
   ```

**That's it!** You should see logs like:
- `celestial-poller | 🚀 Starting Polling Engine...`
- `celestial-dashboard | Network URL: http://0.0.0.0:8501`

---

## 🖥️ Step 3: Access the Dashboard

Open your browser and go to:
👉 **[http://localhost:8501](http://localhost:8501)**

You will see the **Financial Analyst Dashboard** where you can view generated reports and insights.

---

## 🔔 Step 4: Setting up Alerts (Optional)

To get pinged when a new report is ready:

**For Discord:**
1. Go to your Server Settings -> Integrations -> Webhooks.
2. Create New Webhook -> Copy Webhook URL.
3. Paste it into your `.env` file as `NOTIFICATION_WEBHOOK_URL`.

**For Slack:**
1. Go to [Slack Apps](https://api.slack.com/apps) -> Create New App -> Incoming Webhooks.
2. Activate Incoming Webhooks -> Add New Webhook to Workspace.
3. Copy URL -> Paste into `.env`.

---

## 🐛 Troubleshooting

**"GOOGLE_API_KEY not found"**
- Make sure you created the `.env` file in the same folder as `docker-compose.yml` (the root project folder).

**"SEC Rate Limit"**
- The agent respects rate limits, but if you restart too often, you might get a warning. Just wait a minute.

**"Docker not found"**
- Install Docker Desktop from [docker.com](https://www.docker.com/products/docker-desktop/).
