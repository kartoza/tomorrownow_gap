import React, { useState, useEffect } from "react";
import axios from "axios";
import {
  Box,
  Heading,
  Spinner,
  Button,
  Text,
  Table,
  HStack,
  Link as ChakraLink,
} from "@chakra-ui/react";
import { format } from "date-fns";
import { toaster } from "@/components/ui/toaster";

interface Output {
  id: number;
  file_name: string;
  size: number;
  delivered_at: string;
}

const DcasCsvList: React.FC = () => {
  const [outputs, setOutputs] = useState<Output[]>([]);
  const [loading, setLoading] = useState<boolean>(false);

  // pagination state
  const [page, setPage] = useState(1);
  const perPage = 6;
  const totalPages = Math.ceil(outputs.length / perPage);

  useEffect(() => {
    setLoading(true);
    axios
      .get<Output[]>("/outputs/")
      .then((res) => setOutputs(res.data))
      .catch(() =>
        toaster.create({
          title: "Error",
          description: "Failed to load CSV list.",
          type: "error",
        })
      )
      .finally(() => setLoading(false));
  }, []);

  const paginated = outputs.slice((page - 1) * perPage, page * perPage);

  return (
    <Box
      as="section"
      p="7.5"
      bg="white"
      borderRadius="2lg"
      boxShadow="md"
      maxW={{ base: "full", md: "4xl" }}
      mx="auto"
      color="fg"
    >
      <Heading variant="default" size="md" mb="4">
        DCAS CSV Files
      </Heading>

      {loading ? (
        <Spinner size="lg" mt="4" />
      ) : (
        <>
          <Table.ScrollArea>
            <Table.Root size="sm">
              <Table.Header>
                <Table.Row>
                  <Table.ColumnHeader>File name</Table.ColumnHeader>
                  <Table.ColumnHeader textAlign="end">
                    Size (MB)
                  </Table.ColumnHeader>
                  <Table.ColumnHeader>Created on</Table.ColumnHeader>
                  <Table.ColumnHeader />
                </Table.Row>
              </Table.Header>
              <Table.Body>
                {paginated.length > 0 ? (
                  paginated.map((o) => (
                    <Table.Row key={o.id}>
                      <Table.Cell>{o.file_name}</Table.Cell>
                      <Table.Cell textAlign="end">
                        {(o.size / 1_048_576).toFixed(2)}
                      </Table.Cell>
                      <Table.Cell>
                        {format(
                          new Date(o.delivered_at),
                          "yyyy-MM-dd HH:mm"
                        )}
                      </Table.Cell>
                      <Table.Cell>
                        <ChakraLink
                          href={`/outputs/${o.id}/download/`}
                          display="inline-block"
                          _hover={{ textDecoration: "none" }}
                        >
                          <Button visual="solid" size="sm">
                            Download
                          </Button>
                        </ChakraLink>
                      </Table.Cell>
                    </Table.Row>
                  ))
                ) : (
                  <Table.Row>
                    <Table.Cell colSpan={4} textAlign="center" py="4">
                      <Text>No CSV files in the last two weeks.</Text>
                    </Table.Cell>
                  </Table.Row>
                )}
              </Table.Body>
            </Table.Root>
          </Table.ScrollArea>

          {outputs.length > perPage && (
            <HStack justify="space-between" mt="4">
              <Button
                visual="solid"
                size="sm"
                onClick={() => setPage((p) => Math.max(1, p - 1))}
                disabled={page === 1}
              >
                Previous
              </Button>
              <Text>
                Page {page} of {totalPages}
              </Text>
              <Button
                visual="solid"
                size="sm"
                onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                disabled={page === totalPages}
              >
                Next
              </Button>
            </HStack>
          )}
        </>
      )}
    </Box>
  );
};

export default DcasCsvList;
