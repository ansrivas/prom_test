fn main() {
    let a = "/home/user/Downloads/";
    let b = a.split('/').take(20).collect::<Vec<&str>>();
    println!("{:?}", b);
}
