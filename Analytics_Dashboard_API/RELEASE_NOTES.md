# Analytics Dashboard API - Release Notes

## v2.10.0 - 2026-01-07

### Bug Fixes

#### Form Abandonment Calculation
- **Issue**: Dashboard showed 0 abandoned forms despite users dropping off
- **Root Cause**: API counted `FORM_ABANDONED` events which the widget doesn't emit
- **Fix**: Calculate abandoned as `FORM_STARTED - FORM_COMPLETED`

#### Completion Rate Calculation
- **Issue**: Completion rate showed 100% when it should be 50%
- **Root Cause**: Rate was calculated as `completed / (completed + abandoned)` with abandoned = 0
- **Fix**: Calculate as `completed / started`

#### Bottleneck Analysis
- **Issue**: Bottleneck Analysis showed "No bottlenecks detected" despite drop-offs
- **Root Cause**: Function queried non-existent `FORM_ABANDONED` events
- **Fix**: Track sessions with `FORM_STARTED` but no `FORM_COMPLETED`, find last `FORM_FIELD_SUBMITTED` event to identify drop-off field

### New Features

#### Super Admin Tenant Override
- Added `X-Tenant-Override` header support for super_admin users
- Allows viewing other tenants' analytics data
- Requires `role: 'super_admin'` in JWT payload

#### Admin Tenants Endpoint
- New endpoint: `GET /admin/tenants`
- Returns list of active tenants from Bubble
- Restricted to super_admin role only

### Technical Details

**Affected Functions:**
- `fetch_form_summary_from_dynamo()` - Fixed abandoned and rate calculations
- `fetch_form_bottlenecks_from_dynamo()` - Rewritten to detect abandoned sessions
- `authenticate_request()` - Now extracts role from JWT
- `lambda_handler()` - Added tenant override and admin tenants routing

**Deployment:**
```bash
cd Lambdas/lambda/Analytics_Dashboard_API
zip -r deployment.zip . -x "*.pyc" -x "__pycache__/*" -x "*.md"
aws lambda update-function-code --function-name Analytics_Dashboard_API --zip-file fileb://deployment.zip
```
