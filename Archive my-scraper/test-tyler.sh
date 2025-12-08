#!/bin/bash
for county in gregg hardin houston jasper polk rains rusk kaufman lamar limestone llano shelby vanzandt ward wilson; do
  echo -n "$county: "
  curl -I --max-time 8 "https://${county}countytx-web.tylerhost.net" 2>&1 | grep -q "HTTP" && echo "✅ TylerTech" || echo "❌ No response"
  sleep 1
done
