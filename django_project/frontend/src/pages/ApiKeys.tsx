import {
  Box,
  Button,
  Flex,
  Heading,
  Spinner,
  Table,
  useClipboard,
  VisuallyHidden,
} from '@chakra-ui/react';
import { Trash2 } from 'lucide-react';
import { useEffect, useState } from 'react';
import { toaster } from '@/components/ui/toaster';
import { setCSRFToken } from '@/utils/csrfUtils';

interface ApiKey {
  id: string;
  created: string;
  expiry: string | null;
}

export default function ApiKeys() {
  const { onCopy } = useClipboard('');
  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [loading, setLoading] = useState(false);

  /* helpers */
  const fetchKeys = async () => {
    setCSRFToken();
    setLoading(true);
    const res = await fetch('/api/api-keys/', { credentials: 'include' });
    setKeys(await res.json());
    setLoading(false);
  };

  const generate = async () => {
    setCSRFToken();
    const res = await fetch('/api/api-keys/', {
      method: 'POST',
      credentials: 'include',
    });
    const json = await res.json();
    onCopy(json.token);
    toaster({
      title: 'API key generated',
      description: 'Token copied to clipboard — store it safely.',
      status: 'success',
      duration: 8000,
    });
    setKeys(prev => [...prev, { id: json.id, created: json.created, expiry: json.expiry }]);
  };

  const revoke = async (id: string) => {
    await fetch(`/api/api-keys/${id}/`, { method: 'DELETE', credentials: 'include' });
    setKeys(prev => prev.filter(k => k.id !== id));
    toaster({ title: 'API key revoked', status: 'info', duration: 5000 });
  };

  useEffect(() => { void fetchKeys(); }, []);

  /* render */
  return (
    <Box                       /* ← caps width & centres page */
      px={{ base: 4, md: 6 }}
      py={6}
      maxW="4xl"
      mx="auto"
    >
      <Flex
        mb={6}
        justify="space-between"
        align="center"
        flexWrap="wrap"        /* button drops under heading on xs */
      >
        <Heading fontSize={{ base: '2xl', md: '3xl' }}>
          My&nbsp;API&nbsp;Keys
        </Heading>

        {/* button unchanged – relies on theme.ts defaults */}
        <Button
          visual="solid"
          size="md"
          type="submit"
          fontWeight="bold"
          mb={{ base: 4, md: 0 }}
          onClick={generate}
        >
          Generate&nbsp;new
        </Button>
      </Flex>

      {loading ? (
        <Spinner />
      ) : (
        <Table.Root size="sm" striped>
          <Table.Header>
            <Table.Row>
              <Table.ColumnHeader>Created</Table.ColumnHeader>
              <Table.ColumnHeader>Expires</Table.ColumnHeader>
              <Table.ColumnHeader textAlign="end" />
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {keys.map(k => (
              <Table.Row key={k.id}>
                <Table.Cell>{new Date(k.created).toLocaleString()}</Table.Cell>
                <Table.Cell>
                  {k.expiry ? new Date(k.expiry).toLocaleDateString() : '—'}
                </Table.Cell>
                <Table.Cell textAlign="end">
                  <Button
                    size="xs"
                    variant="ghost"
                    onClick={() => revoke(k.id)}
                    aria-label="Delete API key"
                  >
                    <Trash2 size={14} />
                    <VisuallyHidden>Delete</VisuallyHidden>
                  </Button>
                </Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Root>
      )}
    </Box>
  );
}
