import React, { useState } from 'react'
import {
  Button,
  Field,
  Fieldset,
  For,
  Input,
  NativeSelect,
  Stack,
  Checkbox,
  CheckboxGroup,
  RadioGroup,
  Spinner,
  Text,
  Box,
} from '@chakra-ui/react'
import { productList, attributeMap } from '@/utils/measurementData'


const hourlyProducts = ['cbam_shortterm_hourly_forecast']
const windborneProduct = 'windborne_radiosonde_observation'
const salientProduct = 'salient_seasonal_forecast'
const outputTypes = [
  { value: 'json', label: 'JSON' },
  { value: 'csv', label: 'CSV' },
  { value: 'netcdf', label: 'NetCDF' },
  { value: 'ascii', label: 'ASCII (TXT)' },
]

const initialForm = {
  product: '',
  attributes: [] as string[],
  startDate: '',
  endDate: '',
  startTime: '',
  endTime: '',
  forecastDate: '',
  outputType: 'json',
  locationType: 'point' as 'point' | 'bbox' | 'saved',
  lat: '',
  lon: '',
  bbox: { xmin: '', ymin: '', xmax: '', ymax: '' },
  locationName: '',
  altMin: '',
  altMax: '',
}

type FormState = typeof initialForm

type Errors = Partial<Record<keyof FormState, string>>

const MeasurementForm: React.FC = () => {
  const [form, setForm] = useState<FormState>(initialForm)
  const [errors, setErrors] = useState<Errors>({})
  const [loading, setLoading] = useState(false)
  const [jsonResult, setJsonResult] = useState<any>(null)

  const handle = (key: keyof FormState, value: any) => {
    setForm((f) => ({ ...f, [key]: value }))
    setErrors((e) => ({ ...e, [key]: undefined }))
  }

  const validate = (): boolean => {
    const err: Errors = {}
    if (!form.product) err.product = 'Required'
    if (!form.startDate) err.startDate = 'Required'
    if (!form.endDate) err.endDate = 'Required'
    if (form.attributes.length === 0) err.attributes = 'Pick at least one'

    if (windborneProduct === form.product && (!form.altMin || !form.altMax)) {
      err.altMin = 'Provide both altitudes'
    }
    if (salientProduct === form.product && !form.forecastDate) {
      err.forecastDate = 'Required'
    }
    if (hourlyProducts.includes(form.product) && (!form.startTime || !form.endTime)) {
      err.startTime = 'Required'
    }

    if (form.locationType === 'point' && (!form.lat || !form.lon)) {
      err.lat = 'Enter lat & lon'
    }
    if (form.locationType === 'bbox') {
      const { xmin, ymin, xmax, ymax } = form.bbox
      if (!xmin || !ymin || !xmax || !ymax) err.bbox = 'Complete bbox'
    }
    if (form.locationType === 'saved' && !form.locationName) {
      err.locationName = 'Enter a saved name'
    }

    setErrors(err)
    return Object.keys(err).length === 0
  }

  const onSubmit = async () => {
    if (!validate()) return
    setLoading(true)
    setJsonResult(null)
    // build params & fetch…
    setLoading(false)
  }

  const attrs = attributeMap[form.product] || []
  const locationOptions = [
    { label: 'Point', value: 'point' },
    { label: 'BBox', value: 'bbox' },
    { label: 'Saved', value: 'saved' },
  ]

  return (
    <Fieldset.Root size="lg" maxW="800px">
      <Stack mb={4}>
        <Fieldset.Legend>Fetch Measurement Data</Fieldset.Legend>
        <Fieldset.HelperText>
          Choose your parameters and hit Fetch
        </Fieldset.HelperText>
      </Stack>

      <Fieldset.Content>
        {/* Product */}
        <Field.Root invalid={!!errors.product} isRequired>
          <Field.Label>Product</Field.Label>
          <NativeSelect.Root>
            <NativeSelect.Field
              name="product"
              value={form.product}
              onChange={(e) => handle('product', e.target.value)}
            >
              <option value="">Select product</option>
              <For each={productList}>
                {(p) => (
                  <option key={p.variable_name} value={p.variable_name}>
                    {p.name}
                  </option>
                )}
              </For>
            </NativeSelect.Field>
            <NativeSelect.Indicator />
          </NativeSelect.Root>
          <Field.ErrorText>{errors.product}</Field.ErrorText>
        </Field.Root>

        {/* Attributes */}
        <Field.Root invalid={!!errors.attributes} isRequired>
          <Field.Label>Attributes</Field.Label>
          <CheckboxGroup
            value={form.attributes}
            onValueChange={(vals: string[]) => handle('attributes', vals)}
          >
            <Stack>
            <For each={attrs}>
                {(a) => (
                  <Checkbox.Root key={a} value={a}>
                  <Checkbox.HiddenInput />
                  <Checkbox.Control>
                    <Checkbox.Indicator />
                  </Checkbox.Control>
                  <Checkbox.Label>{a}</Checkbox.Label>
                </Checkbox.Root>
                )}
              </For>
            </Stack>
          </CheckboxGroup>
          <Field.ErrorText>{errors.attributes}</Field.ErrorText>
        </Field.Root>

        {/* Dates */}
        <Field.Root invalid={!!errors.startDate} isRequired>
          <Field.Label>Start Date</Field.Label>
          <Input
            type="date"
            value={form.startDate}
            onChange={(e) => handle('startDate', e.target.value)}
          />
          <Field.ErrorText>{errors.startDate}</Field.ErrorText>
        </Field.Root>

        {hourlyProducts.includes(form.product) && (
          <Field.Root invalid={!!errors.startTime} isRequired>
            <Field.Label>Start Time (UTC)</Field.Label>
            <Input
              type="time"
              value={form.startTime}
              onChange={(e) => handle('startTime', e.target.value)}
            />
            <Field.ErrorText>{errors.startTime}</Field.ErrorText>
          </Field.Root>
        )}

        <Field.Root invalid={!!errors.endDate} isRequired>
          <Field.Label>End Date</Field.Label>
          <Input
            type="date"
            value={form.endDate}
            onChange={(e) => handle('endDate', e.target.value)}
          />
          <Field.ErrorText>{errors.endDate}</Field.ErrorText>
        </Field.Root>

        {hourlyProducts.includes(form.product) && (
          <Field.Root invalid={!!errors.endTime} isRequired>
            <Field.Label>End Time (UTC)</Field.Label>
            <Input
              type="time"
              value={form.endTime}
              onChange={(e) => handle('endTime', e.target.value)}
            />
            <Field.ErrorText>{errors.endTime}</Field.ErrorText>
          </Field.Root>
        )}

        {/* Forecast Date */}
        {form.product === salientProduct && (
          <Field.Root invalid={!!errors.forecastDate} isRequired>
            <Field.Label>Forecast Date</Field.Label>
            <Input
              type="date"
              value={form.forecastDate}
              onChange={(e) => handle('forecastDate', e.target.value)}
            />
            <Field.ErrorText>{errors.forecastDate}</Field.ErrorText>
          </Field.Root>
        )}

        {/* Altitudes */}
        {form.product === windborneProduct && (
          <Field.Root invalid={!!errors.altMin} isRequired>
            <Field.Label>Altitudes (min, max)</Field.Label>
            <Stack direction="row">
              <Input
                placeholder="Min"
                value={form.altMin}
                onChange={(e) => handle('altMin', e.target.value)}
              />
              <Input
                placeholder="Max"
                value={form.altMax}
                onChange={(e) => handle('altMax', e.target.value)}
              />
            </Stack>
            <Field.ErrorText>{errors.altMin}</Field.ErrorText>
          </Field.Root>
        )}

        {/* Location Type */}
        <Field.Root>
          <Field.Label>Location Input</Field.Label>
          <RadioGroup.Root
            value={form.locationType}
            onValueChange={(e: any) => handle('locationType', e.value)}
          >
            <Stack gap="6">
              {/* @ts-expect-error */}
              <RadioGroup.Item value="point">
                <RadioGroup.ItemHiddenInput />
                <RadioGroup.ItemIndicator />
                <RadioGroup.ItemText>Point</RadioGroup.ItemText>
              </RadioGroup.Item>

              {/* @ts-expect-error */}
              <RadioGroup.Item value="bbox">
                <RadioGroup.ItemHiddenInput />
                <RadioGroup.ItemIndicator />
                <RadioGroup.ItemText>BBox</RadioGroup.ItemText>
              </RadioGroup.Item>

              {/* @ts-expect-error */}
              <RadioGroup.Item value="saved">
                <RadioGroup.ItemHiddenInput />
                <RadioGroup.ItemIndicator />
                <RadioGroup.ItemText>Saved</RadioGroup.ItemText>
              </RadioGroup.Item>
            </Stack>
          </RadioGroup.Root>
        </Field.Root>

        {/* Output */}
        <Field.Root>
          <Field.Label>Output Format</Field.Label>
          <NativeSelect.Root>
            <NativeSelect.Field
              name="outputType"
              value={form.outputType}
              onChange={(e) => handle('outputType', e.target.value)}
            >
              {outputTypes.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </NativeSelect.Field>
            <NativeSelect.Indicator />
          </NativeSelect.Root>
        </Field.Root>

        <Button
          mt={4}
          alignSelf="flex-start"
          onClick={onSubmit}
          disabled={loading}
        >
          {loading ? <Spinner size="sm" /> : 'Fetch'}
        </Button>

        {jsonResult && (
          <Box mt={4}>
            <Text whiteSpace="pre-wrap">
              {JSON.stringify(jsonResult, null, 2)}
            </Text>
          </Box>
        )}
      </Fieldset.Content>
    </Fieldset.Root>
  )
}

export default MeasurementForm;
