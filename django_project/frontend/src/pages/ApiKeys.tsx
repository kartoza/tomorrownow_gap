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
  Collapsible,
} from '@chakra-ui/react';
import { FiTrash2 as Trash2, FiCalendar } from 'react-icons/fi';
import { useEffect, useState } from 'react';
import { toaster } from '@/components/ui/toaster';
import {
  fetchApiKeys,
  generateApiKey,
  revokeApiKey,
} from '@/features/auth/authSlice';

import { useDispatch, useSelector } from 'react-redux';
import type { RootState, AppDispatch } from '@/app/store';


export default function ApiKeys() {
  const dispatch = useDispatch<AppDispatch>();
  const { setValue, copy } = useClipboard('');
  const keys    = useSelector((s: RootState) => s.auth.apiKeys);
  const loading = useSelector((s: RootState) => s.auth.loading);

   /* dialog */
  const {
    open: isFormOpen,
    onOpen: onFormOpen,
    onClose: onFormClose,
  } = useDisclosure();
  const {
    open: isTokenOpen,
    onOpen: onTokenOpen,
    onClose: onTokenClose,
  } = useDisclosure();
  const [newToken, setNewToken] = useState<string>('');
  const [tokenName, setTokenName] = useState<string>('');
  const [tokenDescription, setTokenDescription] = useState<string>('');
  const [expiry, setExpiry] = useState<string>(() => {
    const d = new Date();
    d.setDate(d.getDate() + 30);
    return d.toISOString().slice(0, 10);
  });

   /* Del dialog */
  const {
    open: delOpen,
    onOpen: onDelOpen,
    onClose: onDelClose,
  } = useDisclosure();
  const [toDeleteId, setToDeleteId] = useState<string | null>(null);

  useEffect(() => {
    dispatch(fetchApiKeys());
  }, [dispatch]);

  const handleFormClose = () => {
    setTokenName("");
    setTokenDescription("");
    setNewToken("");
    onFormClose();
  };

  /* helpers */
  const handleGenerate = async () => {
    try {
      const payload = await dispatch(generateApiKey({
        name: tokenName,
        description: tokenDescription,
        expiry,
      })).unwrap();
      setValue(payload.token);
      copy();
      toaster.create({ title: 'Copied to clipboard', type: 'success' });
      setNewToken(payload.token);
      onTokenOpen();

    } catch {
      toaster.create({ title: 'Generate failed', type: 'error' });
    }
  };
  
  // Revoke handler
  const handleRevoke = async (id: string) => {
    try {
      await dispatch(revokeApiKey(id)).unwrap();
      toaster.create({ title: 'API key revoked', type: 'success' });
    } catch {
      toaster.create({ title: 'Revoke failed', type: 'error' });
    } finally {
      onDelClose();
    }
  };
  

  /* render */
  return (
    <>
      {/* API Page*/}
      <Box
        px={{ base: 4, md: 6 }}
        py={6}
        maxW="4xl"
        mx="auto"
      >
        <Flex
          mb={6}
          justify="space-between"
          align="center"
          flexWrap="wrap"
        >
          <Heading fontSize={{ base: '2xl', md: '3xl' }}>
            My&nbsp;API&nbsp;Keys
          </Heading>

          <Button
            visual="solid"
            size="md"
            type="submit"
            fontWeight="bold"
            mb={{ base: 4, md: 0 }}
            onClick={onFormOpen}
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
                <Table.ColumnHeader>Name</Table.ColumnHeader>
                <Table.ColumnHeader>Description</Table.ColumnHeader>
                <Table.ColumnHeader>Created</Table.ColumnHeader>
                <Table.ColumnHeader>Expires</Table.ColumnHeader>
                <Table.ColumnHeader textAlign="end" />
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {keys.map(k => (
                <Table.Row key={k.id}>
                  <Table.Cell>{k.name}</Table.Cell>
                  <Table.Cell>{k.description || '—'}</Table.Cell>
                  <Table.Cell>{new Date(k.created).toLocaleDateString()}</Table.Cell>
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
      <Dialog.Root open={isFormOpen} onOpenChange={handleFormClose}>
        <Dialog.Backdrop />
        <Dialog.Positioner>
          <Dialog.Content>
            <Dialog.Header>
              <Dialog.Title>Create a new API Key</Dialog.Title>
            </Dialog.Header>

            <Dialog.Body>
              <Input
                value={tokenName}
                placeholder="Token Name"
                mb={2}
                onChange={e => setTokenName(e.target.value)}
                _placeholder={{ color: 'gray.400' }}
                readOnly={!!newToken}
                variant={newToken ? 'subtle' : 'outline'}
              />
              <Input
                value={tokenDescription}
                placeholder="Token Description"
                mb={4}
                onChange={e => setTokenDescription(e.target.value)}
                _placeholder={{ color: 'gray.400' }}
                readOnly={!!newToken}
                variant={newToken ? 'subtle' : 'outline'}
              />
              {/* Expiration date picker */}
              <Input
                type="date"
                value={expiry}
                mb={4}
                onChange={e => setExpiry(e.target.value)}
              />
              <Text mb={2}>
                After you click “Create”, we’ll show you the plaintext once, copy and store it now.
              </Text>

              <Collapsible.Root open={!!newToken}>
                <Collapsible.Content>
                  <Box mt={4} p={4} bg="gray.50" rounded="md">
                    <Text mb={2}>
                      This is the only time you will see it, copy and store it now.
                    </Text>
                    <Input
                      value={newToken}
                      readOnly
                      onFocus={e => e.target.select()}
                    />
                  </Box>
                </Collapsible.Content>
              </Collapsible.Root>
            </Dialog.Body>

            <Dialog.Footer>
              {!newToken ? (
                <>
                  <Button
                    visual="solid"
                    size="xs"
                    mr={3}
                    onClick={handleGenerate}
                    disabled={!tokenName.trim()}
                  >
                    Create
                  </Button>
                  <Button
                    variant="ghost"
                    size="xs"
                    onClick={() => {
                      handleFormClose();
                    }}
                  >
                    Cancel
                  </Button>
                </>
              ) : (
                <>
                  <Button
                    visual="solid"
                    size="xs"
                    mr={3}
                    onClick={() => {
                      copy();
                      toaster.create({ title: 'Copied again', type: 'success' });
                    }}
                  >
                    Copy
                  </Button>
                  <Button
                    variant="ghost"
                    size="xs"
                    onClick={() => {
                      handleFormClose();
                    }}
                  >
                    Close
                  </Button>
                </>
              )}
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
                  if (toDeleteId) handleRevoke(toDeleteId);
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
