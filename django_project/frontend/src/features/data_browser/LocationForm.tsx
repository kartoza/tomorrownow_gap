// src/components/LocationForm.tsx
import React, { useState, ChangeEvent } from 'react'
import {
  Button,
  Field,
  Fieldset,
  Input,
  Stack,
  Spinner,
  Text,
  Box,
} from '@chakra-ui/react'

const LocationForm: React.FC = () => {
  const [locationName, setLocationName] = useState('')
  const [file, setFile] = useState<File | null>(null)
  const [errors, setErrors] = useState<{ name?: string; file?: string }>({})
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<any>(null)
  const [errorMsg, setErrorMsg] = useState<string | null>(null)

  const handleNameChange = (e: ChangeEvent<HTMLInputElement>) => {
    setLocationName(e.target.value)
    setErrors((prev) => ({ ...prev, name: undefined }))
  }

  const handleFileChange = (e: ChangeEvent<HTMLInputElement>) => {
    const selected = e.target.files && e.target.files[0]
    setFile(selected || null)
    setErrors((prev) => ({ ...prev, file: undefined }))
  }

  const validate = (): boolean => {
    const errs: { name?: string; file?: string } = {}
    if (!locationName.trim()) errs.name = 'Location name is required'
    if (!file) errs.file = 'Geometry file is required'
    setErrors(errs)
    return Object.keys(errs).length === 0
  }

  const onSubmit = async () => {
    if (!validate()) return
    setLoading(true)
    setResult(null)
    setErrorMsg(null)

    const params = new URLSearchParams()
    params.set('location_name', locationName)
    const url = `/api/v1/location/?${params.toString()}`

    const formData = new FormData()
    if (file) formData.append('file', file)

    try {
      const resp = await fetch(url, {
        method: 'POST',
        credentials: 'include',
        body: formData,
      })
      if (!resp.ok) {
        const text = await resp.text()
        throw new Error(text)
      }
      const data = await resp.json()
      setResult(data)
    } catch (e: any) {
      setErrorMsg(e.message || 'Unexpected error')
    } finally {
      setLoading(false)
    }
  }

  return (
    <Fieldset.Root size="lg" maxW="md">
      <Stack gap={2} mb={4}>
        <Fieldset.Legend>
          <Text fontSize="md" fontWeight="bold">
            Upload Location
          </Text>
        </Fieldset.Legend>
        <Fieldset.HelperText>
          Provide a name and upload a GeoJSON, zipped Shapefile, or GPKG.
        </Fieldset.HelperText>
      </Stack>

      <Fieldset.Content>
        {errorMsg && (
          <Text color="red.500" mb={2}>
            {errorMsg}
          </Text>
        )}

        <Field.Root invalid={!!errors.name} isRequired>
          <Field.Label>Location Name</Field.Label>
          <Input
            placeholder="e.g. My Farm Boundary"
            value={locationName}
            onChange={handleNameChange}
          />
          <Field.ErrorText>{errors.name}</Field.ErrorText>
        </Field.Root>

        <Field.Root invalid={!!errors.file} isRequired mt={4}>
          <Field.Label>Geometry File</Field.Label>
          <Input
            type="file"
            accept=".geojson,.json,.zip,.gpkg"
            onChange={handleFileChange}
          />
          <Field.HelperText>
            Supported formats: GeoJSON, Shapefile (zip), GPKG
          </Field.HelperText>
          <Field.ErrorText>{errors.file}</Field.ErrorText>
        </Field.Root>

        <Button
          mt={4}
          visual="solid"
          size="sm"
          alignSelf="flex-start"
          onClick={onSubmit}
          disabled={loading}
        >
          {loading ? <Spinner size="sm" /> : 'Upload'}
        </Button>

        {result && (
          <Box mt={4} p={2} bg="gray.50" borderRadius="md">
            <Text fontWeight="bold">Location Saved:</Text>
            <Text whiteSpace="pre-wrap">
              {JSON.stringify(result, null, 2)}
            </Text>
          </Box>
        )}
      </Fieldset.Content>
    </Fieldset.Root>
  )
}

export default LocationForm;
