import io
import os
from typing import Iterable, List, Any
import logging
from itertools import count
from time import sleep, time
from concurrent.futures import Executor, Future
from libraries.thread import BoundedThreadPoolExecutor
from google.cloud import storage
import google.auth
from urllib.parse import quote

logging.basicConfig()
logging.getLogger().setLevel("INFO")
log = logging.getLogger('__name__')

cores = os.cpu_count()
threads = int(cores / 2)
executor = BoundedThreadPoolExecutor(max_workers=threads,
                                     queue_size=int(threads * 1.5))


def compose(object_path: str, slices: List[storage.Blob],
            client: storage.Client, executor: Executor) -> storage.Blob:
    """Compose an object from an indefinite number of slices. Composition will
    be performed single-threaded but using a tree of accumulators to avoid the
    one second object update cooldown period in GCS. Cleanup will be performed
    concurrently using the provided executor.
    Arguments:
        object_path {str} -- The path for the final composed blob.
        slices {List[storage.Blob]} -- A list of the slices which should
            compose the blob, in order.
        client {storage.Client} -- A GCS client to use.
        executor {Executor} -- A concurrent.futures.Executor to use for
            cleanup execution.
    Returns:
        storage.Blob -- The composed blob.
    """
    log.info("Composing")

    chunks = generate_composition_chunks(slices)
    next_chunks = []
    identifier = generate_hex_sequence()

    while len(next_chunks) > 32 or not next_chunks:  # falsey empty list is ok
        for chunk in chunks:
            # make intermediate accumulator
            intermediate_accumulator = storage.Blob.from_string(
                object_path + next(identifier))
            log.info("Intermediate composition: %s", intermediate_accumulator)
            future_iacc = executor.submit(compose_and_cleanup,
                                          intermediate_accumulator, chunk,
                                          client, executor)
            # store reference for next iteration
            next_chunks.append(future_iacc)
        # let intermediate accumulators finish and go again
        chunks = generate_composition_chunks(next_chunks)
        first_pass = False
    # Now can do final compose
    final_blob = storage.Blob.from_string(object_path)
    log.info("Final blob: %s", final_blob.name)
    final_chunk = [blob for sublist in chunks for blob in sublist]
    compose_and_cleanup(final_blob, final_chunk, client, executor, first_pass=first_pass)

    log.info("Composition complete")
    return final_blob


def ensure_results(maybe_futures: List[Any]) -> List[Any]:
    """Pass in a list that may contain Future, and if so, wait for
    the result of the Future and append it; for all other types in the list,
    simply append the value.
    Args:
        maybe_futures (List[Any]): A list which may contain Futures.
    Returns:
        List[Any]: A list with the values passed in, or Future.result() values.
    """
    results = []
    for mf in maybe_futures:
        if isinstance(mf, Future):
            results.append(mf.result())
        else:
            results.append(mf)
    return results


def compose_and_cleanup(blob: storage.Blob, chunk: List[storage.Blob],
                        client: storage.Client, executor: Executor, first_pass=True):
    """Compose a blob and clean up its components. Cleanup tasks will be
    scheduled in the provided executor and the composed blob immediately
    returned.
    Args:
        blob (storage.Blob): The blob to be composed.
        chunk (List[storage.Blob]): The component blobs.
        client (storage.Client): A GCS client.
        executor (Executor): An executor in which to schedule cleanup tasks.
    Returns:
        storage.Blob: The composed blob.
    """
    # wait on results if the chunk is full of futures
    chunk = ensure_results(chunk)
    blob.compose(chunk, client=client)
    # cleanup components created after the initial raw data pass - we no longer need them
    if not first_pass:
        delete_objects_concurrent(chunk, executor, client)
    return blob


def generate_hex_sequence() -> Iterable[str]:
    """Generate an indefinite sequence of hexadecimal integers.
    Yields:
        Iterator[Iterable[str]]: The sequence of hex digits, as strings.
    """
    for i in count(0):
        yield hex(i)[2:]


def delete_objects_concurrent(blobs, executor, client) -> None:
    """Delete GCS objects concurrently.
    Args:
        blobs ([type]): The objects to delete.
        executor ([type]): An executor to schedule the deletions in.
        client ([type]): GCS client to use.
    """
    for blob in blobs:
        log.debug("Deleting slice {}".format(blob.name))
        executor.submit(blob.delete, client=client)
        sleep(.005)  # quick and dirty ramp-up, sorry Dijkstra


def generate_composition_chunks(slices: List,
                                chunk_size: int = 32) -> Iterable[List]:
    """Given an indefinitely long list of blobs, return the list in 32 item chunks.
    Arguments:
        slices {List} -- A list of blobs which are slices of a desired final
            blob.
    Returns:
        Iterable[List] -- An iteration of 31-item chunks of the input list.
    Yields:
        Iterable[List] -- A 31-item chunk of the input list.
    """
    while len(slices):
        chunk = slices[:chunk_size]
        yield chunk
        slices = slices[chunk_size:]


def compose_gcs_objects(
        bucket: str,
        input_dir: str,
        input_top_level_folder: str,
        output_dir: str,
        project_id: str,
        recursive: bool = False):
    client = storage.Client(project=project_id)

    blobs = client.list_blobs(
        bucket,
        prefix=f"{input_dir}/{input_top_level_folder}/",  # <- you need the trailing slash
        delimiter="/",
    )
    next(blobs, ...)  # Force blobs to load
    prefixes = blobs.prefixes
    if prefixes:
        log.info(f"composing for {len(prefixes)} prefixes")
        for prefix in prefixes:
            if recursive:
                delimiter = None
            else:
                delimiter = "/"
            blobs = client.list_blobs(
                bucket,
                prefix=f"{prefix}",  # <- We don't need the trailing slash here since it's included in the prefix
                delimiter=delimiter,
            )
            slices = list(blobs)
            if slices:
                log.debug(f"composing {prefix} {slices}")
                blob_to_compose = f"gs://{bucket}/{output_dir}/{input_top_level_folder}/{prefix.strip('/')}.data"
                compose(object_path=blob_to_compose, slices=slices, client=client, executor=executor)
            else:
                log.warning(f"theres nothing here to compose")
    else:
        log.warning(f"theres nothing here to compose")

