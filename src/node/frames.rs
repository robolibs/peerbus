use super::*;

pub(crate) fn frame_from_datapod<T>(value: &T) -> Vec<u8>
where
    T: datapod::DataPod + 'static,
    <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    <T as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    datapod::to_wire_message(value).bytes
}

pub(crate) fn req_sample_from_frame<Req>(req_id: u64, frame: &[u8]) -> Result<ReqSample<Req>>
where
    Req: datapod::DataPod + 'static,
    <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<Req::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "req frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(ReqSample {
        req_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
}

pub(crate) fn res_sample_from_frame<Res>(req_id: u64, frame: &[u8]) -> Result<ResSample<Res>>
where
    Res: datapod::DataPod + 'static,
    <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<Res::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "res frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(ResSample {
        req_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
}

// ---- que/ans ----

pub(crate) fn que_sample_from_frame<Que>(req_id: u64, frame: &[u8]) -> Result<QueSample<Que>>
where
    Que: datapod::DataPod + 'static,
    <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<Que::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "que frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(QueSample {
        req_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
}

pub(crate) fn ans_sample_from_frame<Ans>(req_id: u64, frame: &[u8]) -> Result<AnsSample<Ans>>
where
    Ans: datapod::DataPod + 'static,
    <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<Ans::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "ans frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(AnsSample {
        req_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
}

// ---- put/ack ----

pub(crate) fn put_sample_from_frame<Put>(req_id: u64, frame: &[u8]) -> Result<PutSample<Put>>
where
    Put: datapod::DataPod + 'static,
    <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<Put::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "put frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(PutSample {
        req_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
}

pub(crate) fn ack_sample_from_frame<Ack>(req_id: u64, frame: &[u8]) -> Result<AckSample<Ack>>
where
    Ack: datapod::DataPod + 'static,
    <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<Ack::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "ack frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(AckSample {
        req_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
}

// ---- pip ----

pub(crate) fn pip_sample_from_frame<T>(session_id: u64, frame: &[u8]) -> Result<PipSample<T>>
where
    T: datapod::DataPod + 'static,
    <T as datapod::DataPod>::Header: datapod::LeWireHeader,
{
    let header_size = std::mem::size_of::<T::Header>();
    if frame.len() < header_size {
        return Err(Error::Remote(format!(
            "pip frame too small: got {} bytes, expected at least {} (header)",
            frame.len(),
            header_size
        )));
    }
    let header = bytemuck::pod_read_unaligned(&frame[..header_size]);
    Ok(PipSample {
        session_id,
        header,
        payload: frame[header_size..].to_vec(),
    })
}

// ---- iroh accept side ----
