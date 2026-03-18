# GitHub Repository Setup Instructions

This document provides step-by-step instructions for creating the GitHub repository and setting up the project board with user stories.

## Step 1: Create GitHub Repository

1. Go to [GitHub](https://github.com) and sign in
2. Click the "+" icon in the top-right corner → "New repository"
3. Repository name: `sgcarmart-used-cars-scraper` (or similar)
4. Description: "Scrape all ~13,800+ used car listings from sgcarmart.com using Cloudflare's `/crawl` API"
5. Choose: Public or Private (based on your preference)
6. Initialize with:
   - [ ] Add a README.md (we already have one)
   - [ ] Add .gitignore (we can add one later)
   - [ ] Add a license (we can add one later)
7. Click "Create repository"

## Step 2: Push Local Code to GitHub

In your terminal, from the project directory:

```bash
# Add the remote origin (replace USERNAME with your GitHub username)
git remote add origin https://github.com/USERNAME/sgcarmart-used-cars-scraper.git

# Verify the remote
git remote -v

# Add all files
git add .

# Commit the initial code
git commit -m "Initial commit: SGCarMart Cloudflare scraper project

- README.md with project overview
- BUSINESS_REQUIREMENTS.md with detailed requirements and user stories
- SCRAPING_PLAN.md with technical implementation plan
- GITHUB_ISSUES.md with formatted user stories for GitHub Issues
- GITHUB_SETUP_INSTRUCTIONS.md (this file)
- Empty cloudflare_scraper.ipynb (to be implemented)
- Existing scraper.ipynb (reference)
- .env template (add your actual credentials locally, never commit)"

# Push to GitHub
git push -u origin main
```

## Step 3: Create GitHub Issues from User Stories

You can create issues manually or use the GitHub CLI (`gh`). Here's how to do it manually:

### Manual Creation:
1. Go to your GitHub repository → Issues → New Issue
2. Copy each issue from `GITHUB_ISSUES.md` and create a separate GitHub issue
3. Add appropriate labels (enhancement, data-engineer, etc.)
4. Assign to yourself or team members as needed

### Using GitHub CLI (if installed):
```bash
# Example: Create the first issue
gh issue create --title "Set up scraping environment quickly" \
  --body "As a Data Engineer, I want to set up the scraping environment quickly so that I can begin development without delays.

**Tasks**:
- Install required Python packages: httpx, polars, beautifulsoup4, python-dotenv, tqdm
- Configure Cloudflare API credentials in .env file
- Validate credentials with test endpoint" \
  --label "enhancement,data-engineer" \
  --assignee @your-username

# Repeat for each issue in GITHUB_ISSUES.md
```

## Step 4: Set Up GitHub Projects (Kanban Board)

1. Go to your GitHub repository → Projects → Link a project
2. Choose "New project" → Select "Board" (Kanban style)
3. Name: "SGCarMart Scraper Development"
4. Description: "Track progress on the SGCarMart used cars data scraping project"
5. Click "Create project"

### Configure Columns:
Add these columns (click "+ Add column"):
1. Backlog
2. To Do
3. In Progress
4. Review
5. Done

### Add Issues to Project:
1. In your project, click "+ Add items"
2. Select "Issues"
3. Check all the issues you created
4. Click "Add items"
5. Drag issues to appropriate columns (typically start all in "Backlog")

## Step 5: Repository Best Practices

### .gitignore
Create a `.gitignore` file with:
```
# Environment variables
.env

# Jupyter notebook checkpoints
.ipynb_checkpoints/

# Output data (can be large)
output/

# Python cache
__pycache__
*.py[cod]

# Local settings
.DS_Store
```

### Branch Protection (Recommended)
1. Go to Settings → Branches → Branch protection rules
2. Add rule for `main` branch
3. Enable:
   - Require pull request reviews before merging
   - Require status checks to pass before merging
   - Include administrators

## Step 6: Development Workflow

1. Create feature branches: `git checkout -b feature/environment-setup`
2. Implement features in notebook or scripts
3. Commit regularly: `git commit -m "feat: implement cloudflare api client"`
4. Push to remote: `git push origin feature/environment-setup`
5. Open Pull Request on GitHub
6. Review and merge to main

## Files to Commit vs Ignore

**Commit these:**
- README.md
- BUSINESS_REQUIREMENTS.md
- SCRAPING_PLAN.md
- GITHUB_ISSUES.md
- GITHUB_SETUP_INSTRUCTIONS.md
- cloudflare_scraper.ipynb (after implementation)
- scraper.ipynb (reference)
- .gitignore

**Never commit (already in .gitignore):**
- .env (contains API credentials)
- output/ directory (large data files)
- .ipynb_checkpoints/

## Next Steps After Setup

1. Implement the cloudflare_scraper.ipynb notebook following the plan in SCRAPING_PLAN.md
2. Test with the small crawl validation (Phase 2)
3. Execute full crawl if validation passes
4. Parse, clean, validate, and export the data
5. Update the repository as you make progress

## Troubleshooting

**Authentication Issues:**
- Use a Personal Access Token (PAT) instead of password for HTTPS
- Or set up SSH keys for authentication

**Large File Warnings:**
- The output/ directory is intentionally ignored to prevent committing large Parquet/CSV files
- Consider using Git LFS if you need to version large files (not recommended for this use case)

**Jupyter Notebook Conflicts:**
- Consider clearing outputs before committing: `Cell → All Output → Clear`
- Or use nbdime for notebook diffing/merging