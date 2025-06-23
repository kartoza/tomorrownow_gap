import {
  Box,
  Button,
  Flex,
  Heading,
  Spinner,
  Table,
  useClipboard,
  VisuallyHidden,
  useDisclosure,
  Dialog,
  Text,
  Input,
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
  const { setValue, copy } = useClipboard('');
  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [loading, setLoading] = useState(false);

   /* dialog */
  const { open, onOpen, onClose } = useDisclosure();
  const [newToken, setNewToken]     = useState<string>('');

   /* Del dialog */
  const {
    open: delOpen,
    onOpen: onDelOpen,
    onClose: onDelClose,
  } = useDisclosure();
  const [toDeleteId, setToDeleteId] = useState<string | null>(null);

  /* helpers */
  const fetchKeys = async () => {
    setCSRFToken();
    setLoading(true);
    const res = await fetch('/api-keys/', { credentials: 'include' });
    setKeys(await res.json());
    setLoading(false);
  };

  const getCookie = (name: string): string => {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop()?.split(";")[0] || "";
    return "";
  };

  const generate = async () => {
    setCSRFToken();
    const res = await fetch('/api-keys/', {
      method: 'POST',
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
        "X-CSRFToken": getCookie("csrftoken"),
      },
    });
    if (!res.ok) {
      toaster.create({ title: 'Generate failed', status: 'error' });
      return;
    }
    const json = await res.json();
    setValue(json.token);
    copy();
    toaster.create({
      title: 'Copied to clipboard',
      type: 'success',
      duration: 5000,
    });

    // show it in the dialog
    setNewToken(json.token);
    onOpen();

    setKeys(prev => [
      ...prev,
      { id: json.id, created: json.created, expiry: json.expiry }
    ]);
  };

  const revoke = async (id: string) => {
    await fetch(`/api-keys/${id}/`, {
      method: 'DELETE',
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
        "X-CSRFToken": getCookie("csrftoken"),
      },
    });
    setKeys(prev => prev.filter(k => k.id !== id));
    toaster.create(
      {
        title: 'API key revoked',
        type: 'success',
        duration: 5000
      });
  };

  useEffect(() => { void fetchKeys(); }, []);

  /* render */
  return (
    <>
      {/* API Page*/}
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
                      aria-label="Delete API key"
                      onClick={() => {
                        setToDeleteId(k.id);
                        onDelOpen();
                      }}
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

      {/* New API key dialog */}
      <Dialog.Root open={open} onOpenChange={onClose}>
        <Dialog.Backdrop />
        <Dialog.Positioner>
          <Dialog.Content>
            <Dialog.Header>
              <Dialog.Title>Your new API Key</Dialog.Title>
            </Dialog.Header>

            <Dialog.Body>
              <Text mb={2}>
                This is the only time you will see it—copy and store it now.
              </Text>
              <Input
                value={newToken}
                readOnly
                onFocus={e => e.target.select()}
              />
            </Dialog.Body>

            <Dialog.Footer>
              <Button
                visual="solid"
                size="xs"
                mr={3}
                onClick={() => {
                  copy();
                  toaster.create({
                    title: 'Copied again',
                    type: 'success',
                    duration: 3000,
                  });
                }}
              >
                Copy
              </Button>
              <Button variant="ghost" size="xs" onClick={onClose}>
                Close
              </Button>
            </Dialog.Footer>
          </Dialog.Content>
        </Dialog.Positioner>
      </Dialog.Root>

      {/* Delete confirmation dialog */}
      <Dialog.Root open={delOpen} onOpenChange={onDelClose}>
        <Dialog.Backdrop />
        <Dialog.Positioner>
          <Dialog.Content>
            <Dialog.Header>
              <Dialog.Title>Confirm Delete</Dialog.Title>
            </Dialog.Header>
            <Dialog.Body>
              <Text>
                Are you sure you want to delete this API key? This action cannot be undone.
              </Text>
            </Dialog.Body>
            <Dialog.Footer>
              <Button variant="ghost" size="xs" onClick={onDelClose}>
                Cancel
              </Button>
              <Button
                visual="solid"
                bg={'red.500'}
                size="xs"
                _hover={{ bg: 'red.600' }}
                ml={3}
                onClick={() => {
                  if (toDeleteId) revoke(toDeleteId);
                  onDelClose();
                }}
              >
                Delete
              </Button>
            </Dialog.Footer>
          </Dialog.Content>
        </Dialog.Positioner>
      </Dialog.Root>
    </>
  );
}
