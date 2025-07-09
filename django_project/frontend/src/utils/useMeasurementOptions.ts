// src/utils/useMeasurementOptions.ts
import { useState, useEffect } from 'react'

export interface Product {
  variable_name: string
  name: string
}

export interface MeasurementOptions {
  products: Product[]
  attributes: Record<string, string[]>
}

/**
 * Fetches the list of dataset‐types (products) and their attributes
 * from GET /api/v1/measurement/options/
 */
export function useMeasurementOptions(): MeasurementOptions & {
  loading: boolean
  error?: string
} {
  const [products, setProducts] = useState<Product[]>([])
  const [attributes, setAttributes] = useState<Record<string, string[]>>({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string>()

  useEffect(() => {
    fetch('/api/v1/measurement/options/', {
      credentials: 'include',
    })
      .then((r) => {
        if (!r.ok) throw new Error(r.statusText)
        return r.json() as Promise<MeasurementOptions>
      })
      .then(({ products: p, attributes: a }) => {
        setProducts(p)
        setAttributes(a)
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  return { products, attributes, loading, error }
}
