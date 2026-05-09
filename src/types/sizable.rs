pub trait Sizable {
    fn size(&self) -> usize;
}

impl Sizable for String {
    fn size(&self) -> usize {
        self.len() + std::mem::size_of::<Self>()
    }
}

impl<T> Sizable for Vec<T> {
    fn size(&self) -> usize {
        self.len() * std::mem::size_of::<T>() + std::mem::size_of::<Self>()
    }
}

impl Sizable for i8 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for i16 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for i32 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for i64 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for i128 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for isize {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for u8 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for u16 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for u32 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for u64 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for u128 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for usize {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for f32 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for f64 {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for bool {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl Sizable for char {
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl<T: Sizable> Sizable for Box<T> {
    fn size(&self) -> usize {
        (**self).size() + std::mem::size_of::<Self>()
    }
}

impl<T: Sizable> Sizable for std::sync::Arc<T> {
    fn size(&self) -> usize {
        (**self).size() + std::mem::size_of::<Self>()
    }
}
