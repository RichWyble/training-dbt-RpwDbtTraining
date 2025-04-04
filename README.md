# RpwDbtTraining
Used for dbt training and learning

## Setup

### dbt Power User Extension

This project includes templates for configuring the dbt Power User extension in both Windsurf and VS Code.

#### For Windsurf Users

1. Copy the template file to your settings file:
   ```
   cp .windsurf/settings.template.json .windsurf/settings.json
   ```

2. Edit the `.windsurf/settings.json` file and replace `YOUR_API_KEY_HERE` with your actual dbt Power User API key.

#### For VS Code Users

1. Copy the template file to your settings file:
   ```
   cp .vscode/settings.template.json .vscode/settings.json
   ```

2. Edit the `.vscode/settings.json` file and replace `YOUR_API_KEY_HERE` with your actual dbt Power User API key.

Both settings files are already included in the `.gitignore` file, so your API key will not be committed to source control.
