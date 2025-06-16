import React, { useState, useEffect } from "react";
import axios from "axios";
import {
  Box,
  Heading,
  Spinner,
  Button,
  Text,
  Table,
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

  const handleDownload = (id: number) => {
    axios
      .get<{ url: string }>(`/outputs/${id}/download/`)
      .then((res) => {
        window.location.href = res.data.url;
      })
      .catch(() =>
        toaster.create({
          title: "Error",
          description: "Could not generate download link.",
          type: "error",
        })
      );
  };

  return (
    <Box
      as="section"
      p={6}
      bg="white"
      borderRadius="md"
      boxShadow="md"
      maxW={{ base: "full", md: "4xl" }}
      mx="auto"
      color="text.primary"
    >
      <Heading mb={2} fontSize="2xl">
        DCAS CSV Files
      </Heading>

      {loading ? (
        <Spinner size="lg" mt={4} />
      ) : (
        <Table.ScrollArea mt={4}>
          <Table.Root>
            <Table.Header>
              <Table.Row>
                <Table.ColumnHeader>File name</Table.ColumnHeader>
                <Table.ColumnHeader textAlign="end">
                  Size (MB)
                </Table.ColumnHeader>
                <Table.ColumnHeader>Created on</Table.ColumnHeader>
                <Table.ColumnHeader /> {/* empty for action column */}
              </Table.Row>
            </Table.Header>

            <Table.Body>
              {outputs.length > 0 ? (
                outputs.map((o) => (
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
                      <Button size="sm" onClick={() => handleDownload(o.id)}>
                        Download
                      </Button>
                    </Table.Cell>
                  </Table.Row>
                ))
              ) : (
                <Table.Row>
                  <Table.Cell
                    colSpan={4}
                    textAlign="center"
                    py="4"
                  >
                    <Text>No CSV files in the last two weeks.</Text>
                  </Table.Cell>
                </Table.Row>
              )}
            </Table.Body>
          </Table.Root>
        </Table.ScrollArea>
      )}
    </Box>
  );
};

export default DcasCsvList;
