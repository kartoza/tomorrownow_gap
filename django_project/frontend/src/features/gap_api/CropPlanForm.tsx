// src/components/CropPlanForm.tsx
import React, { useState } from 'react'
import {
  Button,
  Field,
  Fieldset,
  For,
  Input,
  NativeSelect,
  Stack,
  Spinner,
  Text,
  Box,
} from '@chakra-ui/react'

const CropPlanForm: React.FC = () => {
  const [form, setForm] = useState({
    farmIds: '',
    generatedDate: '',
    forecastFields: '',
    format: 'json',
  })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [jsonResult, setJsonResult] = useState<any>(null)

  const handle = (
    key: keyof typeof form,
    value: string
  ) => {
    setForm((f) => ({ ...f, [key]: value }))
    setError(null)
  }

  const onSubmit = async () => {
    setError(null)
    setJsonResult(null)
    setLoading(true)

    const params = new URLSearchParams()
    if (form.farmIds) params.set('farm_ids', form.farmIds)
    if (form.generatedDate)
      params.set('generated_date', form.generatedDate)
    if (form.forecastFields)
      params.set('forecast_fields', form.forecastFields)
    params.set('format', form.format)

    const url = `/api/v1/crop-plan/?${params.toString()}`
    try {
      if (form.format === 'csv') {
        window.location.href = url
      } else {
        const resp = await fetch(url, { credentials: 'include' })
        if (!resp.ok) throw new Error(await resp.text())
        const data = await resp.json()
        setJsonResult(data)
      }
    } catch (e: any) {
      setError(e.message || 'Unexpected error')
    } finally {
      setLoading(false)
    }
  }

  return (
    <Fieldset.Root size="lg" maxW="md">
      <Stack gap={2} mb={4}>
        <Fieldset.Legend>
          <Text fontSize="md" fontWeight="bold">
            Crop Plan Form
          </Text>
        </Fieldset.Legend>
        <Fieldset.HelperText>
          Query crop plan insight for your farms.
        </Fieldset.HelperText>
      </Stack>

      <Fieldset.Content>
        {error && (
          <Text color="red.500" mb={2}>
            {error}
          </Text>
        )}

        <Field.Root>
          <Field.Label>Farm IDs</Field.Label>
          <Input
            placeholder="Comma-separated IDs"
            value={form.farmIds}
            onChange={(e) => handle('farmIds', e.target.value)}
          />
          <Field.HelperText>
            Comma-separated farm IDs. Leave blank for all farms.
          </Field.HelperText>
        </Field.Root>

        <Field.Root>
          <Field.Label>Generated Date</Field.Label>
          <Input
            type="date"
            value={form.generatedDate}
            onChange={(e) => handle('generatedDate', e.target.value)}
          />
          <Field.HelperText>
            Defaults to today if left blank.
          </Field.HelperText>
        </Field.Root>

        <Field.Root>
          <Field.Label>Forecast Fields</Field.Label>
          <Input
            placeholder="Comma-separated fields"
            value={form.forecastFields}
            onChange={(e) => handle('forecastFields', e.target.value)}
          />
          <Field.HelperText>
            Comma-separated. Leave blank for defaults.
          </Field.HelperText>
        </Field.Root>

        <Field.Root>
          <Field.Label>Format</Field.Label>
          <NativeSelect.Root>
            <NativeSelect.Field
              name="format"
              value={form.format}
              onChange={(e) => handle('format', e.target.value)}
            >
              {['json', 'geojson', 'csv'].map((o) => (
                <option key={o} value={o}>
                  {o.toUpperCase()}
                </option>
              ))}
            </NativeSelect.Field>
            <NativeSelect.Indicator />
          </NativeSelect.Root>
        </Field.Root>

        <Button
          mt={4}
          visual="solid"
          size="sm"
          colorScheme="green.500"
          alignSelf="flex-start"
          onClick={onSubmit}
          disabled={loading}
        >
          {loading ? <Spinner size="sm" /> : 'Fetch'}
        </Button>

        {jsonResult && (
          <Box mt={4} p={2} bg="gray.50" borderRadius="md">
            <Text whiteSpace="pre-wrap">
              {JSON.stringify(jsonResult, null, 2)}
            </Text>
          </Box>
        )}
      </Fieldset.Content>
    </Fieldset.Root>
  )
}

export default CropPlanForm;
