param([ValidateSet('bootstrap','acquire-data','start-replay','test','preflight')]$Command = 'preflight')
switch ($Command) {
  'bootstrap' { python -m pip install -e ".[test]" }
  'acquire-data' { python -m climate_pipeline acquire-data }
  'start-replay' { python -m climate_pipeline start-replay }
  'test' { python -m pytest }
  'preflight' { python -m climate_pipeline preflight }
}

