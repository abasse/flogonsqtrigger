# NSQ Trigger
Flogo trigger activity for NSQ


## Installation

```bash
flogo install github.com/abasse/flogonsqtrigger
```

## Schema
Settings, Outputs and Endpoint:

```json
{
  "output": [
    {
      "name": "message",
      "type": "string"
    }
  ],
  "handler": {
    "settings": [{
      "name": "NsqlookupdAddress",
      "type": "string",
	    "required":"true"
    },
    {
      "name": "Topic",
      "type": "string",
	    "required":"true"
    }
    ]
  }
```
